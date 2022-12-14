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

    @property
    def bblocks_data(self):
        return os.path.join(self.project_dir, "raw_data", "bblocks_data")

    @property
    def raw_oda(self):
        return os.path.join(self.project_dir, "raw_data", "oda")

    @property
    def raw_debt(self):
        return os.path.join(self.project_dir, "raw_data", "debt")

    @property
    def raw_drm(self):
        return os.path.join(self.project_dir, "raw_data", "drm")

    @property
    def root_log(self):
        return os.path.join(self.project_dir, "scripts", "logs")


PATHS = Paths(os.path.dirname(os.path.dirname(__file__)))
