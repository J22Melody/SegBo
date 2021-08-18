import pathlib
import subprocess

from cldfbench import Dataset as BaseDataset


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "segbo"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        from cldfbench import CLDFSpec
        return CLDFSpec(dir=self.cldf_dir, module='StructureDataset')

    def cmd_download(self, args):
        subprocess.check_call(
            'git -C {} submodule update --remote'.format(self.dir.resolve()), shell=True)

    def create_schema(self, ds):
        table = ds.add_table(
            'segbo_with_glottolog.csv',
            {'name': 'ID', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#id"},
            {'name': 'InventoryID'},
        )
        table.tableSchema.primaryKey = ['ID']

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
        self.create_schema(args.writer.cldf)

        counter = 1
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'segbo_with_glottolog.csv',
            dicts=True,
        ):
            value = {
                **{"ID": str(counter)},
                **dict(row)
            }
            args.writer.objects['segbo_with_glottolog.csv'].append(value)
            counter += 1
