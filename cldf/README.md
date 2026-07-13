<a name="ds-structuredatasetmetadatajson"> </a>

# StructureDataset eWAVE

**CLDF Metadata**: [StructureDataset-metadata.json](./StructureDataset-metadata.json)

**Sources**: [sources.bib](./sources.bib)

property | value
 --- | ---
[dc:bibliographicCitation](http://purl.org/dc/terms/bibliographicCitation) | Kortmann, Bernd & Lunkenheimer, Kerstin & Ehret, Katharina (eds.) 2020. The Electronic World Atlas of Varieties of English. Jena: Max Planck Institute for the Science of Human History. (Available online at https://ewave-atlas.org)
[dc:bibliographicCitation ](http://purl.org/dc/terms/bibliographicCitation ) | Kortmann, Bernd & Lunkenheimer, Kerstin (eds.) 2013.
The Electronic World Atlas of Varieties of English.
Leipzig: Max Planck Institute for Evolutionary Anthropology.
(Available online at http://ewave-atlas.org, Accessed on 2018-03-16.)

[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF StructureDataset](http://cldf.clld.org/v1.0/terms.rdf#StructureDataset)
[dc:creator](http://purl.org/dc/terms/creator) | Kortmann, Bernd & Lunkenheimer, Kerstin
[dc:identifier](http://purl.org/dc/terms/identifier) | http://ewave-atlas.org/
[dc:issued](http://purl.org/dc/terms/issued) | 2013-11-15
[dc:license](http://purl.org/dc/terms/license) | http://creativecommons.org/licenses/by/3.0/
[dc:publisher](http://purl.org/dc/terms/publisher) | Max Planck Institute for Evolutionary Anthropology, Leipzig
[dcat:accessURL](http://www.w3.org/ns/dcat#accessURL) | http://ewave-atlas.org/download
[prov:wasDerivedFrom](http://www.w3.org/ns/prov#wasDerivedFrom) | <ol><li><a href="https://github.com/cldf-datasets/ewave/tree/v3.0.1">cldf-datasets/ewave  v3.0.1</a></li><li><a href="https://github.com/glottolog/glottolog/tree/v5.3">Glottolog  v5.3</a></li></ol>
[prov:wasGeneratedBy](http://www.w3.org/ns/prov#wasGeneratedBy) | <ol><li><strong>python</strong>: 3.12.3</li><li><strong>python-packages</strong>: <a href="./requirements.txt">requirements.txt</a></li></ol>
[rdf:ID](http://www.w3.org/1999/02/22-rdf-syntax-ns#ID) | ewave
[rdf:type](http://www.w3.org/1999/02/22-rdf-syntax-ns#type) | http://www.w3.org/ns/dcat#Distribution


## <a name="table-valuescsv"></a>Table [values.csv](./values.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF ValueTable](http://cldf.clld.org/v1.0/terms.rdf#ValueTable)
[dc:extent](http://purl.org/dc/terms/extent) | 18095


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Language_ID](http://cldf.clld.org/v1.0/terms.rdf#languageReference) | `string` | References [languages.csv::ID](#table-languagescsv)
[Parameter_ID](http://cldf.clld.org/v1.0/terms.rdf#parameterReference) | `string` | References [parameters.csv::ID](#table-parameterscsv)
[Value](http://cldf.clld.org/v1.0/terms.rdf#value) | `string` | 
[Code_ID](http://cldf.clld.org/v1.0/terms.rdf#codeReference) | `string` | References [codes.csv::ID](#table-codescsv)
[Comment](http://cldf.clld.org/v1.0/terms.rdf#comment) | `string` | 
[Source](http://cldf.clld.org/v1.0/terms.rdf#source) | list of `string` (separated by `;`) | References [sources.bib::BibTeX-key](./sources.bib)
[Example_ID](http://cldf.clld.org/v1.0/terms.rdf#exampleReference) | list of `string` (separated by ` `) | References [examples.csv::ID](#table-examplescsv)

## <a name="table-examplescsv"></a>Table [examples.csv](./examples.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF ExampleTable](http://cldf.clld.org/v1.0/terms.rdf#ExampleTable)
[dc:extent](http://purl.org/dc/terms/extent) | 4189


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Language_ID](http://cldf.clld.org/v1.0/terms.rdf#languageReference) | `string` | References [languages.csv::ID](#table-languagescsv)
[Primary_Text](http://cldf.clld.org/v1.0/terms.rdf#primaryText) | `string` | 
[Analyzed_Word](http://cldf.clld.org/v1.0/terms.rdf#analyzedWord) | list of `string` (separated by `\t`) | 
[Gloss](http://cldf.clld.org/v1.0/terms.rdf#gloss) | list of `string` (separated by `\t`) | 
[Translated_Text](http://cldf.clld.org/v1.0/terms.rdf#translatedText) | `string` | 
[Meta_Language_ID](http://cldf.clld.org/v1.0/terms.rdf#metaLanguageReference) | `string` | References [languages.csv::ID](#table-languagescsv)
[Comment](http://cldf.clld.org/v1.0/terms.rdf#comment) | `string` | 
[Source](http://cldf.clld.org/v1.0/terms.rdf#source) | list of `string` (separated by `;`) | References [sources.bib::BibTeX-key](./sources.bib)

## <a name="table-codescsv"></a>Table [codes.csv](./codes.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF CodeTable](http://cldf.clld.org/v1.0/terms.rdf#CodeTable)
[dc:extent](http://purl.org/dc/terms/extent) | 1410


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Parameter_ID](http://cldf.clld.org/v1.0/terms.rdf#parameterReference) | `string` | References [parameters.csv::ID](#table-parameterscsv)
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Description](http://cldf.clld.org/v1.0/terms.rdf#description) | `string` | 
`Number` | `integer` | 

## <a name="table-parameterscsv"></a>Table [parameters.csv](./parameters.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF ParameterTable](http://cldf.clld.org/v1.0/terms.rdf#ParameterTable)
[dc:extent](http://purl.org/dc/terms/extent) | 235


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Description](http://cldf.clld.org/v1.0/terms.rdf#description) | `string` | 
`Category_ID` | `string` | References [featurecategories.csv::ID](#table-featurecategoriescsv)
`Example_Source` | `string` | 
`Attestation` | `float` | Attestation is a relative measure of how widespread a feature is in the set of eWAVE varieties. It is expressed as a percentage and is calculated as the sum of all A-, B- and C-ratings for a feature, divided by the number of varieties in the eWAVE dataset. The closer the value to 100%, the more widespread the feature is.
`Pervasiveness` | `float` | Pervasiveness provides a measure of how pervasive a feature is on average in the varieties in  which it is attested. Pervasiveness is calculated as all A-ratings for a feature plus 0.6 times  the B-ratings for the same feature plus 0.3 times the C-ratings, divided by the sum of all  A-, B- and C-ratings for the feature. This value is then multiplied by 100 and expressed as a  percentage. A Pervasiveness value of 100% or close to 100% thus indicates that the feature is  highly pervasive (rated A) in all or most of the varieties for which it is attested, while a  value close to 30% (the lowest possible value) indicates that the feature is extremely rare  (rated C) in most or all of the varieties for which it is attested. Intermediate values are less  easy to interpret – here one has to look more closely at the ratio of A- to B- to C-values.  Two more things should also be noted here:  - The Pervasiveness value does not provide information on how widespread a feature is in the entire    eWAVE dataset, i.e. for how many varieties the feature is actually attested. - Since the eWAVE contributors did not all use exactly the same strategies in deciding when to    give a feature an A- vs. a B- or a C- vs. a B- rating, it is very difficult to translate the    ratings into numerical values that adequately reflect the differences between A-, B- and    C-ratings. The choice made here (1 for A, 0.6 for B and 0.3 for C) is certainly only one of    many, and further testing is required to see how adequate this model is. 

## <a name="table-languagescsv"></a>Table [languages.csv](./languages.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF LanguageTable](http://cldf.clld.org/v1.0/terms.rdf#LanguageTable)
[dc:extent](http://purl.org/dc/terms/extent) | 77


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Macroarea](http://cldf.clld.org/v1.0/terms.rdf#macroarea) | `string` | 
[Latitude](http://cldf.clld.org/v1.0/terms.rdf#latitude) | `decimal`<br>&ge; -90<br>&le; 90 | 
[Longitude](http://cldf.clld.org/v1.0/terms.rdf#longitude) | `decimal`<br>&ge; -180<br>&le; 180 | 
[Glottocode](http://cldf.clld.org/v1.0/terms.rdf#glottocode) | `string`<br>Regex: `[a-z0-9]{4}[1-9][0-9]{3}` | 
[ISO639P3code](http://cldf.clld.org/v1.0/terms.rdf#iso639P3code) | `string`<br>Regex: `[a-z]{3}` | 
`Description` | `string` | 
`Region_ID` | `string` | References [regions.csv::ID](#table-regionscsv)
`Type_ID` | `string` | References [varietytypes.csv::ID](#table-varietytypescsv)
`abbr` | `string` | 
`Contributor_ID` | list of `string` (separated by ` `) | References [contributors.csv::ID](#table-contributorscsv)

## <a name="table-regionscsv"></a>Table [regions.csv](./regions.csv)

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 8


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 

## <a name="table-varietytypescsv"></a>Table [varietytypes.csv](./varietytypes.csv)

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 5


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
`Description` | `string` | 

## <a name="table-featurecategoriescsv"></a>Table [featurecategories.csv](./featurecategories.csv)

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 12


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
`Description` | `string` | 

## <a name="table-contributorscsv"></a>Table [contributors.csv](./contributors.csv)

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 87


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
`URL` | `string` | 
`Address` | `string` | 
`Email` | `string` | 

## <a name="table-historycsv"></a>Table [history.csv](./history.csv)

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 550


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
`Version` | `string` | 
`Language_ID` | `string` | References [languages.csv::ID](#table-languagescsv)
`Parameter_ID` | `string` | References [parameters.csv::ID](#table-parameterscsv)
`Code_ID` | `string` | References [codes.csv::ID](#table-codescsv)
