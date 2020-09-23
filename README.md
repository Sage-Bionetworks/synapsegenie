# Synapse Genie

## Introduction

This package can deploy a AACR GENIE like project on Synapse and perform validation and processing of files.

## Dependencies

These are tools or packages you will need, to be able to reproduce these results:
- Python 3.6 or higher
- [synapseclient](http://python-docs.synapse.org) (`pip install synapseclient`)
- Python [pandas](http://pandas.pydata.org) (`pip install pandas`)
- Synapse Project configured to work with synapsegenie

## Creating your own registry

Please view the [example registry](example_registry) to learn how to utilize `synapsegenie`.  `synapsegenie` allows a user to create a registry package with a list of file formats.  Each of these file format classes should extend `synapsegenie.example_filetype_format.FileTypeFormat`.  Learn more about creating Python packages [here](https://packaging.python.org/tutorials/packaging-projects/).  Once you have installed your registry package, you can now use the `synapsegenie` command line client.

## File Validator
```
pip install synapsegenie
synapsegenie -v
```

This will install all the necessary components for you to run the validator locally on all of your files, including the Synapse client.  Please view the help to see how to run to validator.

```
synapsegenie validate-single-file -h
```


# Contributing

To learn how to contribute, please read the [contributing guide](CONTRIBUTING.md)
