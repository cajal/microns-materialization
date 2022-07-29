import wridgets.app as wra
from ..schemas import \
    minnie65_materialization as m65mat

class MatApp(wra.App):
    store_config = [
        'value',
        ('options', [
            ('latest', m65mat.Materialization.latest.fetch(
                'ver', order_by='ver DESC').tolist()),
            ('stable', m65mat.Materialization.long_term_support.fetch(
                'ver', order_by='ver DESC').tolist()),
            ('all', m65mat.Materialization.CAVE.fetch(
                'ver', order_by='ver DESC').tolist())
        ])
    ]

    def make(self, on_select=None, **kwargs):
        self.propagate = True

        label_kws = dict(
            name='MatLabel',
            text='Materialization',
            output=self.output,
        )
        buttons_kws = dict(
            name='MatButtons',
            options=self.options,
            on_interact=self.update_dropdown_options,
            output=self.output
        )
        dropdown_kws = dict(
            name='MatDropdown',
            options=self.options[0][1],
            on_interact=self.set_value,
            output=self.output
        )

        select_kws = dict(
            name='MatSelectButton',
            description='Select',
            button_style='info',
            on_interact=on_select
        )

        self.core = wra.Label(**label_kws) + wra.SelectButtons(**buttons_kws) + \
            wra.Dropdown(**dropdown_kws) + wra.ToggleButton(**select_kws)
        self.set_value()

    def update_dropdown_options(self):
        self.children.MatDropdown.set(
            options=self.children.MatButtons.get1('value'))

    def set_value(self):
        self.value = self.children.MatDropdown.get1('value')
