import pathlib
import subprocess
import unicodedata
import hashlib
from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec

# copy from https://github.com/cldf-datasets/phoible/blob/phoible-3.0/cldfbench_phoible.py#L91
def glang_attrs(glang, languoids):
    """
    Enrich language metadata with attributes we can fetch from Glottolog.
    """
    res = {k: None for k in 'Macroarea'.split(',')}

    if not glang.macroareas:
        if glang.level.name == 'dialect':
            for _, gc, _ in reversed(glang.lineage):
                if languoids[gc].macroareas:
                    res['Macroarea'] = languoids[gc].macroareas[0].name
                    break
    else:
        res['Macroarea'] = glang.macroareas[0].name

    return res

class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "segbo"
    valueTableProperties = ['OnlyInLoanwords', 'Result', 'NewDistinction', 'PhonemeComments']
    languageTableProperties = ['family_id', 'parent_id', 'bookkeeping', 'level', 'status', 'description', 'markup_description', 'child_family_count', 'child_language_count', 'child_dialect_count', 'country_ids']
    inventoryTableProperties = ['BibTexKey', 'Filename', 'MetadataComments', 'PhoibleID', 'ClosestNeighbor']

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir=self.cldf_dir, module='StructureDataset')

    def cmd_download(self, args):
        subprocess.check_call(
            'git -C {} submodule update --remote'.format(self.dir.resolve()), shell=True)

    def create_schema(self, ds):
        # values.csv
        ds.remove_columns('ValueTable', 'Code_ID', 'Comment', 'Source')
        ds.add_columns(
            'ValueTable',
            {
                "dc:extent": "multivalued",
                "separator": ",",
                "datatype": "string",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#glottocode",
                "name": "Source_Language_ID",
            },
            {
                "dc:extent": "singlevalued",
                "datatype": "string",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#contributionReference",
                "required": True,
                "name": "Inventory_ID"
            },
            *self.valueTableProperties,
        )

        # parameters.csv
        ds.add_component('ParameterTable')

        # languages.csv
        ds.add_component('LanguageTable', *self.languageTableProperties)

        # contributions.csv
        ds.add_component('ContributionTable')
        ds.remove_columns('ContributionTable', 'Name', 'Description', 'Citation')
        ds.add_columns(
            'ContributionTable',
            {'name': 'Language_ID', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#languageReference"},
            {'name': 'Language_Name', 'propertyUrl': "http://cldf.clld.org/v1.0/terms.rdf#name"},
            *self.inventoryTableProperties,
        )
        ds.add_foreign_key('ValueTable', 'Inventory_ID', 'ContributionTable', 'ID')

    def cmd_makecldf(self, args):
        self.create_schema(args.writer.cldf)

        # values.csv
        counter = 1
        plist = []
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'SegBo database - Phonemes.csv',
            dicts=True,
        ):
            desc = ' - '.join(unicodedata.name(c) for c in row['BorrowedSound'])
            pid = hashlib.md5(desc.encode('utf8')).hexdigest().upper()

            args.writer.objects['ValueTable'].append({
                'ID': str(counter),
                'Parameter_ID': pid,
                'Inventory_ID': row['InventoryID'],
                'Language_ID': row['BorrowingLanguageGlottocode'],
                'Source_Language_ID': list(filter(lambda x: x != 'unknown', row['SourceLanguageGlottocode'].split(', '))),
                'Inventory_ID': row['InventoryID'],
                'Value': row['BorrowedSound'],
                **{ k: row[k] for k in self.valueTableProperties}
            })
            # parameters.csv
            if pid not in plist:
                args.writer.objects['ParameterTable'].append({
                    'ID': pid,
                    'Name': row['BorrowedSound'],
                    'Description': desc, 
                })
            counter += 1
            plist.append(pid)
        
        # languages.csv
        glangs = {l.id: l for l in args.glottolog.api.languoids()}
        language_ids = list(map(lambda row: row['Language_ID'], args.writer.objects['ValueTable']))
        source_language_ids = list(map(lambda row: row['Source_Language_ID'], args.writer.objects['ValueTable']))
        source_language_ids = [item for sublist in source_language_ids for item in sublist]
        
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'glottolog_languoid.csv' / 'languoid.csv',
            dicts=True,
        ):
            if row['id'] in language_ids or row['id'] in source_language_ids:
                args.writer.objects['LanguageTable'].append({
                    'ID': row['id'],
                    'Name': row['name'],
                    'Glottocode': row['id'],
                    'ISO639P3code': row['iso639P3code'],
                    'Latitude': row['latitude'],
                    'Longitude': row['longitude'],
                    **(glang_attrs(glangs[row['id']], glangs) if row['id'] in glangs else {}),
                    **{ k: row[k] for k in self.languageTableProperties}
                })

        # contributions.csv
        for row in self.raw_dir.read_csv(
            self.raw_dir / 'segbo' / 'data' / 'SegBo database - Metadata.csv',
            dicts=True,
        ):
            args.writer.objects['ContributionTable'].append({
                'ID': row['InventoryID'],
                'Language_ID': row['Glottocode'],
                'Language_Name': row['LanguageName'],
                'Contributor': row['Contributor'],
                **{ k: row[k] for k in self.inventoryTableProperties}
            })
