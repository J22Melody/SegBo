import pathlib

from cldfbench import Dataset as BaseDataset


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "SegBo"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        from cldfbench import CLDFSpec
        return CLDFSpec(dir=self.cldf_dir, module='StructureDataset')

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        self.raw_dir.download('https://raw.githubusercontent.com/segbo-db/segbo/master/data/segbo_with_glottolog.csv', 'segbo_with_glottolog.csv')

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
            'segbo_with_glottolog.csv',
            dicts=True,
        ):
            value = {
                **{"ID": str(counter)},
                **dict(row)
            }
            args.writer.objects['segbo_with_glottolog.csv'].append(value)
            counter += 1
