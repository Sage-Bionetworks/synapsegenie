# Synapse Genie

## Introduction

This package can deploy a AACR GENIE like project on Synapse and perform validation and processing of files.

## Installation

Dependencies:
- Python 3.6 or higher
- [synapseclient](http://python-docs.synapse.org) (`pip install synapseclient`)
- Python [pandas](http://pandas.pydata.org) (`pip install pandas`)

```
pip install synapsegenie
synapsegenie -v
```

## Usage

### Creating your own registry
Please view the [example registry](example_registry) to learn how to utilize `synapsegenie`.  `synapsegenie` allows a user to create a registry package with a list of file formats.  Each of these file format classes should extend `synapsegenie.example_filetype_format.FileTypeFormat`.  Learn more about creating Python packages [here](https://packaging.python.org/tutorials/packaging-projects/).  Once you have installed your registry package, you can now use the `synapsegenie` command line client.

### synapsegenie Synapse project
A `synapsegenie` Synapse project must exist for you to fully utilize this package.  There is now a command to create this infrastructure in Synapse.

```
synapsegenie bootstrap-infra --format_registry_packages example_registry
```

### File Validator
This will install all the necessary components for you to run the validator locally on all of your files, including the Synapse client.  Please view the help to see how to run to validator.

```
synapsegenie validate-single-file -h
```


## Contributing

To learn how to contribute, please read the [contributing guide](CONTRIBUTING.md)
