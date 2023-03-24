import collections
import glob
import socket
import subprocess

from middlewared.service import Service


class MseriesNvdimmService(Service):

    class Config:
        private = True
        namespace = 'mseries.nvdimm'

    def run_ixnvdimm(self, nvmem_dev):
        base = f'ixnvdimm -r {nvmem_dev}'
        cmds = [
            f'{base} SLOT0_FWREV',
            f'{base} SLOT1_FWREV',
            f'{base} FW_SLOT_INFO',
            f'{base} NVM_LIFETIME',
            f'{base} ES_LIFETIME',
            f'{base} SPECREV',
            f'{base} MODULE_HEALTH',
            f'{base} ES_TEMP',
            f'{base} ARM_STATUS',
            f'{base} MODULE_HEALTH_STATUS',
        ]
        return subprocess.run(
            ';'.join(cmds),
            stdout=subprocess.PIPE,
            shell=True,
            encoding="utf-8",
            errors="ignore",
        ).stdout.strip().split('\n')

    def critical_health_status(self, value):
        crit_hlth_mapping = (
            (0x01, 'PERSISTENCY_LOST_ERROR'),
            (0x02, 'WARNING_THRESHOLD_EXCEEDED'),
            (0x04, 'PERSISTENCY_RESTORED'),
            (0x08, 'BELOW_WARNING_THRESHOLD'),
            (0x10, 'PERMANENT_HARDWARE_FAILURE'),
            (0x20, 'EVENT_N_LOW'),
        )
        crit_hlth_hex = f'0x{value}'
        crit_hlth_int = int(crit_hlth_hex, 16)
        crit_hlth_info = {crit_hlth_hex: []}
        for _, msg in filter(lambda x: x[0] & crit_hlth_int, crit_hlth_mapping):
            crit_hlth_info[crit_hlth_hex].append(msg)

        return crit_hlth_info

    def arm_info(self, value):
        arm_mapping = (
            (0x01, 'SUCCESS'),
            (0x02, 'ERROR'),
            (0x04, 'SAVE_N_ARMED'),
            (0x08, 'RESET_N_ARMED'),
            (0x10, 'ABORT_SUCCESS'),
            (0x20, 'ABORT_ERROR'),
        )
        arm_status_hex = f'0x{value}'
        arm_status_int = int(arm_status_hex, 16)
        arm_status_info = {arm_status_hex: []}
        for _, msg in filter(lambda x: x[0] & arm_status_int, arm_mapping):
            arm_status_info[arm_status_hex].append(msg)

        return arm_status_info

    def es_temp(self, value):
        # Workaround wrong units reported by Micron NVDIMMs
        es_temp = int(f'0x{value}', 16)
        if es_temp & 0x1000 != 0:
            es_temp = -(es_temp & 0x0fff) // 16
        elif es_temp >= 128:
            es_temp = (es_temp & 0x0fff) // 16

        return es_temp

    def module_health_status(self, value):
        mod_mapping = (
            (0x0001, 'VOLTAGE_REGULATOR_FAILED'),
            (0x0002, 'VDD_LOST'),
            (0x0004, 'VPP_LOST'),
            (0x0008, 'VTT_LOST'),
            (0x0010, 'DRAM_NOT_SELF_REFRESH'),
            (0x0020, 'CONTROLLER_HARDWARE_ERROR'),
            (0x0040, 'NVM_CONTROLLER_ERROR'),
            (0x0080, 'NVM_LIFETIME_ERROR'),
            (0x0100, 'NOT_ENOUGH_ENERGRY_FOR_CSAVE'),
            (0x0200, 'INVALID_FIRMWARE_ERROR'),
            (0x0400, 'CONFIG_DATA_ERROR'),
            (0x0800, 'NO_ES_PRESENT'),
            (0x1000, 'ES_POLICY_NOT_SET'),
            (0x2000, 'ES_HARDWARE_FAILURE'),
            (0x4000, 'ES_HEALTH_ASSESSMENT_ERROR'),
        )
        mod_hex = f'0x{value}'
        mod_int = int(mod_hex, 16)
        mod_info = {mod_hex: []}
        for _, msg in filter(lambda x: x[0] & mod_int, mod_mapping):
            mod_info[mod_hex].append(msg)

        return mod_info

    def parse_ixnvdimm_output(self, data):
        return {
            'critical_health_info': self.critical_health_status(data[6]),
            'module_health_info': self.module_health_status(data[9]),
            'running_firmware': '.'.join(data[0][:2] if data[2][-1] == '0' else data[1][:2]),
            'nvm_lifetime_percent': int(f'0x{data[3]}', 16),
            'es_lifetime_percent': int(f'0x{data[4]}', 16),
            'es_current_temperature': self.es_temp(data[7]),
            'arm_status': self.arm_info(data[8]),
            'specrev': int(data[5]),
        }

    def get_vendor_info(self, nvmem_dev):
        info = (
            'vendor', 'device', 'rev_id',
            'subsystem_vendor', 'subsystem_device', 'subsystem_rev_id',
            'serial',
        )
        vendor_info = collections.OrderedDict([(i, '') for i in info])
        for filename in info:
            try:
                with open(f'/sys/bus/nd/devices/{nvmem_dev}/nfit/{filename}') as f:
                    value = int(f.read().strip(), 16)
                    if filename == 'serial':
                        vendor_info[filename] = hex(socket.ntohl(value)).removeprefix('0x')
                    else:
                        vendor_info[filename] = hex(socket.ntohs(value))
            except (ValueError, FileNotFoundError):
                pass

        mapping = {
            '0x2c80_0x4e32_0x31_0x3480_0x4131_0x1': {
                'part_num': '18ASF2G72PF12G6V21AB',
                'size': '16GB', 'clock_speed': '2666MHz',
                'qualified_firmare': ['2.1', '2.2', '2.4'],
            },
            '0x2c80_0x4e36_0x31_0x3480_0x4231_0x2': {
                'part_num': '18ASF2G72PF12G9WP1AB',
                'size': '16GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['2.2'],
            },
            '0x2c80_0x4e33_0x31_0x3480_0x4231_0x1': {
                'part_num': '36ASS4G72PF12G9PR1AB',
                'size': '32GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['2.4'],
            },
            '0xc180_0x4e88_0x33_0xc180_0x4331_0x1': {
                'part_num': 'AGIGA8811-016ACA',
                'size': '16GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['0.8'],
            },
            '0xce01_0x4e39_0x34_0xc180_0x4331_0x1': {
                'part_num': 'AGIGA8811-032ACA',
                'size': '32GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['0.8'],
            },
            'unknown': {
                'part_num': None,
                'size': None, 'clock_speed': None,
                'qualified_firmware': [],
            }
        }
        key = '_'.join([v for k, v in vendor_info.items() if k != 'serial'])
        vendor_info.update(mapping.get(key, mapping['unknown']))
        return vendor_info

    def info(self):
        results = []
        sys = ("TRUENAS-M40", "TRUENAS-M50", "TRUENAS-M60")
        if not self.middleware.call_sync("truenas.get_chassis_hardware").startswith(sys):
            return results

        try:
            for nmem in glob.glob("/dev/nmem*"):
                info = {'dev': nmem.removeprefix('/dev/'), 'dev_path': nmem}
                info.update(self.get_vendor_info(info['dev']))
                info.update(self.parse_ixnvdimm_output(self.run_ixnvdimm(nmem)))
                results.append(info)
        except Exception:
            self.logger.error("Unhandled exception obtaining nvdimm info", exc_info=True)
        else:
            return results
