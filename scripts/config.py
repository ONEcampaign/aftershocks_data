import os


class Paths:
    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def charts(self):
        return os.path.join(self.project_dir, "charts_live")

    @property
    def download(self):
        return os.path.join(self.project_dir, "charts_download")

    @property
    def raw_data(self):
        return os.path.join(self.project_dir, "raw_data")


PATHS = Paths(os.path.dirname(os.path.dirname(__file__)))
