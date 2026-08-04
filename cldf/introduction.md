# Introduction

by Kortmann, Bernd & Lunkenheimer, Kerstin & Ehret, Katharina


## History and people behind eWAVE

As a follow-up of the interactive CD-ROM accompanying the Mouton de Gruyter Handbook of Varieties of English (Kortmann, Schneider et al., eds. 2004), it was in 2008 that Bernd Kortmann and Kerstin Lunkenheimer joined forces in designing a considerably larger and more fine-grained database that could be used as an even more informative, less L1-centred research tool in comparative cross-dialectal and cross-varietal studies trying to map grammatical variation in the Anglophone world. In close collaboration with leading experts on the different variety types (including Lieselotte Anderwald, Susanne Michaelis, Rajend Mesthrie, Peter Mühlhäusler, Jeff Siegel, Peter Trudgill and Susanne Wagner) successive versions of a questionnaire were designed and discussed, before the version underlying eWAVE, with its 235 morphosyntactic features, was decided upon and sent out to the informants. One reason for (a) considerably extending the scope of morphosyntactic variation to be documented and (b) choosing the programming format was that right from the start eWAVE was meant to allow immediate comparisons with other large databases on grammatical variation, notably WALS (The World Atlas of Language Structures; Dryer and Haspelmath, eds. 2011) and APiCS Online (Atlas of Pidgin and Creole Structures Online; Michaelis, Maurer, Haspelmath, Huber, eds. 2013), both developed at the  Max Planck Institute for Evolutionary Anthropology (Leipzig). It was a unique opportunity that the same people who were trusted with programming APiCS were also allowed to spend a significant amount of their time programming eWAVE.


## The 77 varieties, Pidgins and Creoles in the eWAVE sample

[The LanguageTable](languages.csv) provides an overview of the varieties, Pidgins and Creoles sampled in eWAVE and their distribution across variety types and world regions.


## Variety types covered

The 77 data sets in the eWAVE sample fall into 5 broad classes of variety types, listed and briefly characterized below. The experts were asked to classify “their” variety as belonging to one of these variety types.


### Traditional L1 varieties (L1t)

The varieties in this group are traditional, regional non-standard varieties which are long-established mother-tongue varieties and are characterized by a relatively low degree of contact with other dialects or other languages since the beginning of the colonial period (i.e. within approximately the last 400 years).


### High-contact L1 varieties (L1c)

As the label suggests, the varieties in this group are characterized by a high degree of contact between different dialects of English and/or between English and other languages. The following three types of varieties are considered high-contact L1 varieties in eWAVE:

1. Transplanted L1 Englishes or colonial standards, i.e. relatively new indigenized varieties that arose roughly within the last 400 years, had native speakers from early on and were formed by settlers with diverse linguistic and/or dialectal backgrounds. They typically emerged in territories that are former settlement colonies, and some, e.g. New Zealand English or Australian English, have developed an independent standard that is increasingly being recognized both within the community and elsewhere. Others, such as Bahamian English, or Channel Islands English, are closer in status to traditional regional dialects.
2. Language-shift Englishes, i.e. varieties that have replaced the erstwhile primary language in the community and that have adult and child L1 and L2 speakers forming one speech community. Some of these varieties, e.g. Irish English and Welsh English, also have shifted entirely, and do no longer have significant numbers of L2 speakers.
3. Standard L1 varieties


### Indigenized L2 varieties (L2)

We use this label to refer to two types of non-native varieties. The first and larger group are non-native indigenized varieties that emerged in territories where English was introduced in the colonial era, typically via the education system, and is still used in education and other official domains, but where L1 speakers of metropolitan varieties were never present in significant numbers. These varieties usually do not have significant numbers of native speakers, but many enjoy some degree of prestige and normative status in their political communities. An example in eWAVE is Pakistani English.

The second group of L2 varieties are non-native varieties spoken in territories where L1 speakers of English are (or used to be) present in significant numbers, or even form the majority of the population, but contact between L1 and L2 speakers nevertheless is (or used to be) limited. These varieties are ‘indigenized’ to the extent that they are recognizable as distinct varieties, but they typically do not have prestige or normative status. Examples of this type in the eWAVE set are Chicano English in the US and the English of the Black and Indian ethnic communities in South Africa.


### English-based Pidgins (P)

English-based contact varieties (or rather, contact languages) that typically developed in trade colonies for the purpose of communication between two or more groups of speakers that did not share a common language. Full acquisition of the English language has in most cases not been the target. Initially, pidgins are nobody’s mother tongue and are usually restricted to certain domains of use (often as lingua francas). However, they may over time acquire native speakers and also enter further domains of use (extended/expanded pidgins). With the exception of Butler English, all the English-based pidgins in eWAVE can be considered expanded pidgins.


### English-based Creoles (Cr)

English-based contact varieties (or rather: contact languages) that typically developed in settings (often in plantation colonies) where a group of non-English speakers acquired some variety of English. Typically, there was strong pressure upon the non-English speaking group to use the language of the socio-economically superior group (i.e. English), while exposure to its native speakers was normally very limited. In the Caribbean, for example, the proportion of native (and L2) speakers of English was rather low in contrast to non-English speakers (who constantly arrived in the colonies in large numbers). Many creoles have become the native language of the majority of the population.


## Domains of grammar covered in eWAVE

The 235 features in eWAVE cover phenomena from 12 different grammatical domains. An overview is provided in [the ParameterTable](parameters.csv).


## Feature ratings

The information in the eWAVE database consists of judgements by top experts on each of the 77 varieties, Pidgins and Creoles on the frequency with which each of the 235 features can be encountered in the relevant variety, Pidgin, or Creole. The following classifications were used:

Rating | Description
--- | ---
A	| feature is pervasive or obligatory
B	| feature is neither pervasive nor extremely rare
C	| feature exists, but is extremely rare
D	| attested absence of feature
X	| feature is not applicable (given the structural make-up of the variety/P/C)
?	| no information on feature is available


## eWAVE statistics


### 1. Attestation

Attestation is a relative measure of how widespread a feature is in the set of eWAVE varieties. It is expressed as a percentage and is calculated as the sum of all A-, B- and C-ratings for a feature, divided by the number of varieties in the eWAVE dataset. The closer the value to 100%, the more widespread the feature is.


### 2. Pervasiveness

Pervasiveness provides a measure of how pervasive a feature is on average in the varieties in which it is attested. Pervasiveness is calculated as all A-ratings for a feature plus 0.6 times the B-ratings for the same feature plus 0.3 times the C-ratings, divided by the sum of all A-, B- and C-ratings for the feature. This value is then multiplied by 100 and expressed as a percentage. A Pervasiveness value of 100% or close to 100% thus indicates that the feature is highly pervasive (rated A) in all or most of the varieties for which it is attested, while a value close to 30% (the lowest possible value) indicates that the feature is extremely rare (rated C) in most or all of the varieties for which it is attested. Intermediate values are less easy to interpret – here one has to look more closely at the ratio of A- to B- to C-values. Two more things should also be noted here:

1. The Pervasiveness value does not provide information on how widespread a feature is in the entire eWAVE dataset, i.e. for how many varieties the feature is actually attested.
2. Since the eWAVE contributors did not all use exactly the same strategies in deciding when to give a feature an A- vs. a B- or a C- vs. a B- rating, it is very difficult to translate the ratings into numerical values that adequately reflect the differences between A-, B- and C-ratings. The choice made here (1 for A, 0.6 for B and 0.3 for C) is certainly only one of many, and further testing is required to see how adequate this model is.


## Limitations and research potential of feature ratings

It should be obvious that in large-scale surveys such as eWAVE, APiCS or WALS, feature ratings need to be taken with a grain of salt. What looks categorical can hardly be more than an abstraction of and approximation to linguistic and social reality. Each of the varieties, Pidgins and Creoles included in eWAVE is itself subject to internal variation so that the profile emerging from the WAVE questionnaire for a given variety is unlikely to perfectly match the linguistic behaviour of any particular subgroup of speakers of that variety (e.g. different age groups). This is particularly true for the English-based Pidgins and Creoles and for the L2 varieties in the database. Typically, they have ethnically and socially diverse speech communities, so that features attested in WAVE may not be present in some speakers, or may be present with a different frequency, depending on which other languages they speak, and whether they are mesolectal, acrolectal or basilectal speakers. WAVE also glosses over regional variation in large speech communities like India or North America, when it subsumes them under one variety (‘Indian English’ and ‘Colloquial American English’). Moreover, the frequency-based ratings in most cases reflect the individual expert’s judgements since for many of the varieties, Pidgins and Creoles corpus data are not available at all, or only to a limited extent.

There is an enormous research potential behind each of these caveats, pointing to the fact that eWAVE is at least as much a starting-point for new research as it is the outcome of prior research. For example, for anyone working within variationist sociolinguistics or within the emerging field of variationist pragmatics (especially the pragmatics of grammar) it will be fascinating to zoom in on the individual data points of the eWAVE feature set. Especially promising in this respect are all the features rated 'B' or 'C' since they are the prime candidates for glossing over 'orderly heterogeneity'.


## The future of eWAVE

The database eWAVE is 'e' in a double sense: it is electronic and it is e-volving. It is designed as something dynamic, as a constantly growing and improving teaching and research tool.  Thus, since the launch of eWAVE 1.0 in November 2011, quite a few changes and improvements have been made. With regard to the data, the most important of these are a number of corrections to feature ratings, the addition of two completely new data sets (Philippine English and Cape Flats English), and more than 2,400 new examples. Other new features include a list of references, export of filtered tables, and changes in filtering tools, navigation and general layout to fit in with the general architecture of APiCS and WALS.

We plan to continue updating and expanding eWAVE in the future, and as before we welcome the comments and suggestions of the international research community and all users. This will help eWAVE to continually gain in reliability, strength and richness as a tool in academic teaching and research.


## Acknowledgements

There are many people and institutions, to some extent also lucky coincidences, eWAVE would have been impossible without. To start with, the two editors gratefully acknowledge the support of the  Freiburg Institute for Advanced Studies (FRIAS) in the design and data collection phase of the project. Bernd Kortmann enjoyed an Internal Senior Fellowship at the FRIAS from April 2008 until September 2009 and Kerstin Lunkenheimer joined him there as a research assistant from September 2008 until March 2009. Kerstin and Bernd are also most indebted to the Max Planck Institute for Evolutionary Anthropology (Leipzig), particularly to Susanne Michaelis and Martin Haspelmath for much helpful advice, for letting us piggyback on their projects, for offering to host eWAVE on the MPI server and, above all, for giving permission to their brilliant in-house linguist programmers Hagen Jung, Hans-Jörg Bibiko and Robert Forkel to work on eWAVE alongside APiCS. Hagen did a fantastic job in the early stages, calmly listened to all the ideas on what this new research tool was meant to do and never tired in providing improved versions of eWAVE and searching for optimal solutions, often surprising the editors with new ideas, fascinating visualizations and query options that they had never thought possible. Hans-Jörg and Robert made sure that everything continued to work as both eWAVE and APiCS evolved, and made the transition from eWAVE 1.0 to eWAVE 2.0 as smooth as we could only wish.

Above all, the editors would like to thank the 83 collaborators serving as informants for this project. Without their readiness to devote a significant amount of their precious time to filling in the questionnaire, providing examples and answering our questions, eWAVE would have been impossible. A crucial motivation for making eWAVE available as an open access resource thus was that eWAVE should be owned by the entire research community, as a dynamic resource and platform that continues to be improved and expanded and that will serve as a point of reference as much as a point of departure in teaching and research on varieties of English all around the world.


## References

- Kortmann, Bernd, Edgar W. Schneider, Kate Burridge, Rajend Mesthrie and Clive Upton, eds. 2004. The Handbook of Varieties of English. A multimedia reference tool. Two volumes plus CD-ROM. Berlin/New York: Mouton de Gruyter.
- Dryer, Matthew S. and Martin Haspelmath,eds. 2011. The World Atlas of Language Structures Online. Munich: Max Planck Digital Library. Available online at http://wals.info/ accessed 2011-07-28.
- Michaelis, Susanne Maria, Philippe Maurer, Martin Haspelmath and Magnus Huber, eds. 2013. Atlas of Pidgin and Creole Language Structures Online. Leipzig: Max Planck Institute for Evolutionary Anthropology. (Available online at http://apics-online.info Accessed on 2013-11-04.)
