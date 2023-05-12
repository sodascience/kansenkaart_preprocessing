# KansenKaart preprocessing
*Pipeline to create cohorts for the KansenKaart project*

![pipeline.png](pipeline.png)

A reproducible preprocessing pipeline for the [KansenKaart]( https://kansenkaart.nl/) project. The input is raw microdata from CBS (or a synthetic test-version from our repository [cbs_validationdata](https://github.com/sodascience/cbs_validationdata)). The output is several tidy, cleaned-up cohort data sets. The cohort data sets serve as the input to the [kansenkaart analysis](https://github.com/sodascience/kansenkaart_analysis).

## What is KansenKaart?
[KansenKaart]( https://kansenkaart.nl/) is a project on intergenerational mobility across areas in the Netherlands. The goal is to show how circumstances in which Dutch individuals grew up are related to later-in-life outcomes. 

We start by measuring the average outcomes in later life, such as income, for children in each neighborhood, considering different demographic subgroups (such as gender, migration background, and the number of parents in the household they grew up in) and socioeconomic status (based on parental income). Our focus is on the municipalities, neighborhoods, and postal codes where the children grew up, regardless of their current place of residence. Subsequently, we analyze these geographical disparities in later-life outcomes across various areas in the Netherlands. The final results are now accessible on the interactive website [KansenKaart.nl](https://kansenkaart.nl/), which provides information on a wide range of outcomes, including health, education, and economic productivity.

So far, we have obtained a total of 64 outcomes from seven different cohorts. These cohorts have been generated utilizing the repository provided. For more comprehensive information about the cohorts, please refer to the available details [here](https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/documentation/COHORTS.md). The final estimates of these outcomes are created using the repository [kansenkaart analysis](https://github.com/sodascience/kansenkaart_analysis).

## Installation
- Clone this repository to a folder on your machine
- Unzip the latest version of the built CBS data into the `/cbsdata` folder from [here](https://github.com/sodascience/cbs_validationdata/releases) OR edit the file locations in the `config` files to point to the right files.
- Open the `.Rproj` file, install the `renv` package
- Run `renv::restore()` to install the right versions of all dependencies

## Cohort creation/ usage
The cohort data sets are created using the file `create_cohort_data.R`. At the top of this file, change the desired input configuration to one of the following:
- `config/main.yml`
- `config/students.yml`
- `config/high_school.yml`
- `config/elementary_school.yml`
- `config/classroom.yml`
- `config/perinatal.yml`
- `config/child_mortality.yml`

After this change, run the entire file to create the cohort data set in the scratch folder!

# Details

## Configuration
Several cohorts can be created using this repository, from the raw microdata files at CBS. The configuration files for each of these cohorts can be found in the [`config`](./config) folder. Several selections and/or date values can also be changed there.

## Preparation pipeline
The data preparation pipeline has four components:

1. Cohort creation
    - Selecting the cohort based on filtering criteria. 
    - Adding parent information to the cohort.
    - Adding postal code and region information to the cohort.
    - Writing `01_cohort.rds` to the scratch folder.
2. Predictor creation
    - Adding parent income and income percentile to the cohort.
    - Adding migration background information to the cohort.
    - Writing `02_predictor.rds` to the scratch folder.
3. Outcome creation
    - Adding outcomes of children to the cohort.
    - Writing `03_outcomes.rds` to the scratch folder.
4. Post-processing
    - Selecting variables of interest.
    - Writing `kansenkaart_data.rds` to the scratch folder.

More information on these four components can be found [here]( https://github.com/sodascience/kansenkaart_preprocessing/blob/cbs_updated/resources/documentation/COMPONENTS.md). 

## Package management
The CBS server has specific versions of packages. A package version dump is available in the `resources` folder. For package management, we use `renv`. This ensures the same versions are installed as in CBS.

To use a new package, install it using `install.packages()`, and then run the file `resources/set_cbs_versions.R`. This will ensure that your package exists at CBS (it will warn & remove if it does not) and also that the package is at the right version.

## License and citation

The `KansenKaart preprocessing` pipeline is published under the MIT license. When using for academic work, please cite:

```
Citation for the KansenKaart preprocessing
```

## Contact
This project is developed and maintained by the [ODISSEI Social Data Science
(SoDa)](https://odissei-data.nl/nl/soda/) team.

Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact [Erik-Jan van
Kesteren](https://github.com/vankesteren)
([@ejvankesteren](https://twitter.com/ejvankesteren)) or [Helen Lam](https://github.com/Helenlam20).
