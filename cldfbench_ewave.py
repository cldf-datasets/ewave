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
        raw_ds = StructureDataset.from_metadata(self.raw_dir / 'StructureDataset-metadata.json')

        args.writer.objects['contributions.csv'] = list(raw_ds['contributions.csv'])
        args.writer.objects['varietytypes.csv'] = [
            {'ID': r[0], 'Name': r[1], 'Description': r[2]}
            for r in self.raw_dir.read_csv('varietytype.psv', delimiter='|')]
        args.writer.objects['featurecategories.csv'] = [
            {'ID': r[0], 'Name': r[1]}
            for r in self.raw_dir.read_csv('featurecategory.psv', delimiter='|')]
        args.writer.objects['regions.csv'] = [
            {'ID': r[0], 'Name': r[1]} for r in self.raw_dir.read_csv('region.psv', delimiter='|')]

        # We add columns to some tables.
        ds = args.writer.cldf
        ds.add_sources(self.raw_dir.read('sources.bib'))
        ds.add_table('regions.csv', 'ID', 'Name')
        ds.add_table('varietytypes.csv', 'ID', 'Name', 'Description')
        ds.add_table('featurecategories.csv', 'ID', 'Name', 'Description')

        # Varieties have a region and a type and an abbreviation.
        ds.add_columns(
            'LanguageTable', 'Region_ID', 'Type_ID', 'abbr'
        )
        ds['LanguageTable'].add_foreign_key('Region_ID', 'regions.csv', 'ID')
        ds['LanguageTable'].add_foreign_key('Type_ID', 'varietytypes.csv', 'ID')

        # Features have a category and a typical example, with source.
        ds.add_columns(
            'ParameterTable', 'Category_ID', 'Example_Source',
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
        # Examples may have sources:
        ds.add_columns(
            'ExampleTable',
            {
                'name': 'Source',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#source',
                'separator': ';',
            }
        )

        data = {r[0]: r[1:] for r in self.raw_dir.read_csv('variety.csv')}
        for row in raw_ds['LanguageTable']:
            row['Region_ID'] = data[row['ID']][0]
            row['Type_ID'] = data[row['ID']][1]
            row['abbr'] = data[row['ID']][2]
            args.writer.objects['LanguageTable'].append(row)

        data = {r[0]: r[1:] for r in self.raw_dir.read_csv('feature.csv')}
        for row in raw_ds['ParameterTable']:
            row['Example_Source'] = data[row['ID']][0]
            row['Category_ID'] = data[row['ID']][1]
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
