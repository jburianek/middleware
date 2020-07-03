from middlewared.schema import Dict, Str

from .device import Device
from .utils import create_element


class CDROM(Device):

    schema = Dict(
        'attributes',
        Str('path', required=True),
    )

    def xml_freebsd(self, *args, **kwargs):
        child_element = kwargs.pop('child_element')
        return create_element(
            'disk', type='file', device='cdrom', attribute_dict={
                'children': [
                    create_element('driver', name='file', type='raw'),
                    create_element('source', file=self.data['attributes']['path']),
                    create_element('target', dev=f'hda{self.data["id"]}', bus='sata'),
                    child_element,
                ]
            }
        )
