import pathlib
import itertools

from pycldf import StructureDataset, Reference
from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec, Metadata

URL = "https://cdstar.shh.mpg.de/bitstreams/EAEA0-B49C-97B6-CA70-0/ewave_dataset.cldf.zip"


class MetadataWithTravis(Metadata):
    def markdown(self):
        lines, title_found = [], False
        for line in super().markdown().split('\n'):
            lines.append(line)
            if line.startswith('# ') and not title_found:
                title_found = True
                lines.extend([
                    '',
                    "[![Build Status](https://travis-ci.org/cldf-datasets/ewave.svg?branch=master)]"
                    "(https://travis-ci.org/cldf-datasets/ewave)"
                ])
        return '\n'.join(lines)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "ewave"
    metadata_cls = MetadataWithTravis

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module='StructureDataset',
            default_metadata_path=self.raw_dir / 'StructureDataset-metadata.json')

    def cmd_download(self, args):
        self.raw_dir.download_and_unpack(URL)

    def cmd_makecldf(self, args):
        #
        # Augment the schema of the rather simplistic CLDF download:
        #
        ds = args.writer.cldf
        # Add tables for controlled vocabularies:
        ds.add_table('regions.csv', 'ID', 'Name')
        ds.add_table('varietytypes.csv', 'ID', 'Name', 'Description')
        ds.add_table('featurecategories.csv', 'ID', 'Name', 'Description')
        ds.add_table('contributors.csv', 'ID', 'Name', 'URL', 'Address', 'Email')

        # We merge the data from contributions.csv into languages.csv for simplicity:
        ds.remove_table('contributions.csv')

        # Varieties have a region, a type, an abbreviation and contributors.
        ds.add_columns(
            'LanguageTable',
            'Description',
            'Region_ID',
            'Type_ID',
            'abbr',
            {'name': 'Contributor_ID', 'separator': ' '})
        ds['LanguageTable'].add_foreign_key('Region_ID', 'regions.csv', 'ID')
        ds['LanguageTable'].add_foreign_key('Type_ID', 'varietytypes.csv', 'ID')
        ds['LanguageTable'].add_foreign_key('Contributor_ID', 'contributors.csv', 'ID')

        # Features have a category and a typical example, with source.
        ds.add_columns(
            'ParameterTable',
            'Category_ID',
            'Example_Source',
            {
                'name': 'Attestation',
                'datatype': 'float',
                'dc:description':
                    "Attestation is a relative measure of how widespread a feature is in the set "
                    "of eWAVE varieties. It is expressed as a percentage and is calculated as the "
                    "sum of all A-, B- and C-ratings for a feature, divided by the number of "
                    "varieties in the eWAVE dataset. The closer the value to 100%, the more "
                    "widespread the feature is.",
            },
            {
                'name': 'Pervasiveness',
                'datatype': 'float',
                'dc:description': """\
Pervasiveness provides a measure of how pervasive a feature is on average in the varieties in 
which it is attested. Pervasiveness is calculated as all A-ratings for a feature plus 0.6 times 
the B-ratings for the same feature plus 0.3 times the C-ratings, divided by the sum of all 
A-, B- and C-ratings for the feature. This value is then multiplied by 100 and expressed as a 
percentage. A Pervasiveness value of 100% or close to 100% thus indicates that the feature is 
highly pervasive (rated A) in all or most of the varieties for which it is attested, while a 
value close to 30% (the lowest possible value) indicates that the feature is extremely rare 
(rated C) in most or all of the varieties for which it is attested. Intermediate values are less 
easy to interpret – here one has to look more closely at the ratio of A- to B- to C-values. 
Two more things should also be noted here:

- The Pervasiveness value does not provide information on how widespread a feature is in the entire 
  eWAVE dataset, i.e. for how many varieties the feature is actually attested.
- Since the eWAVE contributors did not all use exactly the same strategies in deciding when to 
  give a feature an A- vs. a B- or a C- vs. a B- rating, it is very difficult to translate the 
  ratings into numerical values that adequately reflect the differences between A-, B- and 
  C-ratings. The choice made here (1 for A, 0.6 for B and 0.3 for C) is certainly only one of 
  many, and further testing is required to see how adequate this model is.
""",
            },
        )
        ds['ParameterTable'].add_foreign_key('Category_ID', 'featurecategories.csv', 'ID')

        # Values may have (many) examples:
        ds.add_columns(
            'ValueTable',
            {
                'name': 'Example_ID',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference',
                'separator': ' ',
            }
        )
        # ... but no Contribution_ID anymore:
        ds.remove_columns('ValueTable', 'Contribution_ID')

        # Examples may have sources:
        ds.add_columns(
            'ExampleTable',
            {
                'name': 'Source',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#source',
                'separator': ';',
            }
        )

        history = ds.add_table('history.csv', 'Version', 'Language_ID', 'Parameter_ID', 'Code_ID')
        history.add_foreign_key('Language_ID', 'languages.csv', 'ID')
        history.add_foreign_key('Parameter_ID', 'parameters.csv', 'ID')
        history.add_foreign_key('Code_ID', 'codes.csv', 'ID')

        #
        # Now add the data:
        #
        ds.add_sources(self.raw_dir.read('sources.bib'))

        args.writer.objects['varietytypes.csv'] = [
            {'ID': r[0], 'Name': r[1], 'Description': r[2]}
            for r in self.raw_dir.read_csv('varietytype.psv', delimiter='|')]
        args.writer.objects['featurecategories.csv'] = [
            {'ID': r[0], 'Name': r[1], 'Description': r[2]}
            for r in self.raw_dir.read_csv('featurecategory.psv', delimiter='|')]
        args.writer.objects['regions.csv'] = [
            {'ID': r[0], 'Name': r[1]} for r in self.raw_dir.read_csv('region.psv', delimiter='|')]

        for lid, pid, cid, _ in self.raw_dir.read_json('changes.json')['2013']:
            args.writer.objects['history.csv'].append({
                'Version': '1.0',
                'Language_ID': lid,
                'Parameter_ID': pid,
                'Code_ID': '{0}-{1}'.format(pid, cid.replace('?', 'NA'))
            })

        for row in self.raw_dir.read_csv('contributors.csv', dicts=True):
            #id, name, url, email, address
            args.writer.objects['contributors.csv'].append({
                'ID': row['id'],
                'Name': row['name'],
                'URL': row['url'],
                'Email': row['email'],
                'Address': row['address'],
            })

        # We read the bulk of the data from the CLDF export of the website:
        raw_ds = StructureDataset.from_metadata(self.raw_dir / 'StructureDataset-metadata.json')

        cc = {
            cid: [r[1] for r in rows] for cid, rows in itertools.groupby(
                sorted(
                    self.raw_dir.read_csv('cc.csv'),
                    key=lambda r: (int(r[0]), int(r[2]), int(r[1]))),
                lambda r: r[0],
            )
        }
        desc = {
            r['ID']: r['Description']
            for r in self.raw_dir.read_csv('contributions.csv', dicts=True)}
        data = {r[0]: r[1:] for r in self.raw_dir.read_csv('variety.csv')}
        for row in raw_ds['LanguageTable']:
            row['Region_ID'] = data[row['ID']][0]
            row['Type_ID'] = data[row['ID']][1]
            row['abbr'] = data[row['ID']][2]
            row['Description'] = desc[row['ID']]
            row['Contributor_ID'] = cc[row['ID']]
            args.writer.objects['LanguageTable'].append(row)

        data = {r[0]: r[1:] for r in self.raw_dir.read_csv('feature.csv')}
        for row in raw_ds['ParameterTable']:
            row['Example_Source'] = data[row['ID']][0]
            row['Category_ID'] = data[row['ID']][1]
            row['Attestation'] = data[row['ID']][2]
            row['Pervasiveness'] = data[row['ID']][3]
            args.writer.objects['ParameterTable'].append(row)

        # Augment examples.csv
        def ref(r):
            return str(Reference(r['source'], r['description'].replace('[', '(').replace(']', ')')))

        examplesource = {
            eid: [ref(r) for r in rows]
            for eid, rows in itertools.groupby(
                sorted(
                    self.raw_dir.read_csv('examplesource.csv', dicts=True),
                    key=lambda d: (int(d['example']), d['source'])),
                lambda d: d['example']
            )
        }
        for row in raw_ds['ExampleTable']:
            row['Source'] = examplesource.get(row['ID'], [])
            args.writer.objects['ExampleTable'].append(row)

        # Renumber codes and values!
        for row in raw_ds['CodeTable']:
            row['ID'] = '{0}-{1}'.format(row['Parameter_ID'], row['Name'].replace('?', 'NA'))
            args.writer.objects['CodeTable'].append(row)

        valuesentence = {
            vid: [r['sentence'] for r in rows]
            for vid, rows in itertools.groupby(
                sorted(
                    self.raw_dir.read_csv('valueexample.csv', dicts=True),
                    key=lambda d: (int(d['value']), int(d['sentence']))),
                lambda d: d['value']
            )
        }

        for row in raw_ds['ValueTable']:
            row['Example_ID'] = valuesentence.get(row['ID'], [])
            row['ID'] = '{0}-{1}'.format(row['Language_ID'], row['Parameter_ID'])
            row['Code_ID'] = '{0}-{1}'.format(row['Parameter_ID'], row['Value'] or 'NA')
            args.writer.objects['ValueTable'].append(row)
