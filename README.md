# notgroupb workerHygon

## Overview
Worker detecting Flooding based from Hygon Data Points based on provided characteristic values:

 - **MHW**: means highest  water level,
 - **MW**: Average of water levels in a time span,
 - **MNW**: mean lowest value of water levels in a period of time

## Getting Started
### Prerequisites
 - [Apache Maven](https://maven.apache.org/) for packaging.
### Installing

```bash
mvn install
```
 A jar File will be created in the `/target` directory.

### Deployment
The Worker is provided as an executable jar with included dependencies and can be run from the command line:
```bash
java -jar workerHygon-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

## Versioning

We use [Semantic Versioning](http://semver.org/) for versioning.

## Authors

* **Thorben Kraft** - *Initial work* - [TeKraft](https://github.com/TeKraft)
* **Jan Speckamp** - *Small Updates* - [speckij](https://github.com/speckij)

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details
