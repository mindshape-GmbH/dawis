from jinja2 import Environment, FileSystemLoader, select_autoescape


class TemplateRenderer:
    def __init__(self, path_templates: str = 'resources/templates', file_extensions: tuple = ('html', 'txt')):
        self._environment = Environment(
            loader=FileSystemLoader(path_templates),
            autoescape=select_autoescape(file_extensions),
            extensions=['jinja2.ext.loopcontrols']
        )

        self.add_filter('datetime', lambda x, y='%Y-%m-%dT%H:%M:%S%z': x.strftime(y))

    def render_template(self, template: str, variables: dict):
        return self._environment.get_template(template).render(**variables)

    def add_filter(self, key: str, func: callable):
        self._environment.filters[key] = func
