import pathlib
import subprocess

from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "segbo"
    valueTableProperties = ['OnlyInLoanwords', 'Result', 'NewDistinction', 'PhonemeComments', 'Verified']
    languageTableProperties = ['family_id', 'parent_id', 'bookkeeping', 'level', 'status', 'description', 'markup_description', 'child_family_count', 'child_language_count', 'child_dialect_count', 'country_ids']

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir=self.cldf_dir, module='StructureDataset')

    def cmd_download(self, args):
        subprocess.check_call(
            'git -C {} submodule update --remote'.format(self.dir.resolve()), shell=True)

    def create_schema(self, ds):
        # values.csv
        ds.remove_columns('ValueTable', 'Code_ID', 'Comment')
        ds.add_columns(
            'ValueTable',
            {
                "dc:extent": "singlevalued",
                "datatype": "string",
                # FIXME: ValueError: Duplicate column property: http://cldf.clld.org/v1.0/terms.rdf#languageReference
                # "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#languageReference",
                "required": True,
                "name": "Source_Language_ID",
            },
            'Inventory_ID',
            *self.valueTableProperties,
        )

        # parameters.csv
        table = ds.add_component('ParameterTable')

        # languages.csv
        ds.add_component('LanguageTable', *self.languageTableProperties)

        # inventories.csv
        table = ds.add_table(
            'inventories.csv',
            {'name': 'ID', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#id"},
            {'name': 'Language_ID', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#languageReference"},
            {'name': 'Language_Name', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#name"},
            {'name': 'Source', 'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#source', 'separator': ';'},
        )
        table.tableSchema.primaryKey = ['ID']
        ds.add_foreign_key('ValueTable', 'Inventory_ID', 'inventories.csv', 'ID')

    def cmd_makecldf(self, args):
        self.create_schema(args.writer.cldf)

        # values.csv
        counter = 1
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'SegBo database - Phonemes.csv',
            dicts=True,
        ):
            args.writer.objects['ValueTable'].append({
                'ID': str(counter),
                'Parameter_ID': str(counter),
                'Inventory_ID': row['InventoryID'],
                'Language_ID': row['BorrowingLanguageGlottocode'],
                'Source_Language_ID': row['SourceLanguageGlottocode'],
                'Inventory_ID': row['InventoryID'],
                'Value': row['BorrowedSound'],
                'Source': '', # TODO 
                **{ k: row[k] for k in self.valueTableProperties}
            })
            # parameters.csv
            args.writer.objects['ParameterTable'].append({
                'ID': str(counter),
                'Name': row['BorrowedSound'],
                'Description': '', # TODO
                # TODO more features, do not exist in raw data
            })
            counter += 1
        
        # languages.csv
        language_ids = list(map(lambda row: row['Language_ID'], args.writer.objects['ValueTable']))
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'glottolog_languoid.csv' / 'languoid.csv',
            dicts=True,
        ):
            if row['id'] in language_ids:
                args.writer.objects['LanguageTable'].append({
                    'ID': row['id'],
                    'Name': row['name'],
                    'Macroarea': '', # TODO, do not exist in raw data
                    'Latitude': row['latitude'],
                    'Longitude': row['longitude'],
                    'Glottocode': row['id'],
                    'ISO639P3code': row['iso639P3code'],
                    **{ k: row[k] for k in self.languageTableProperties}
                })

        # inventories.csv
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'SegBo database - Metadata.csv',
            dicts=True,
        ):
            args.writer.objects['inventories.csv'].append({
                'ID': row['InventoryID'],
                'Language_ID': row['Glottocode'],
                'Language_Name': row['LanguageName'],
                'Source': '', # TODO source table?
                # TODO count phonemes like in done in phoible?
            })
