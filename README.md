# Efsbeat

Welcome to Efsbeat.

This is a simple beat to get any folder(s) size every period of time. It has specific implementation on [how AWS EFS meters usage](http://docs.aws.amazon.com/efs/latest/ug/metered-sizes.html), so every event will have multiple size values, one called `size.real` and one called `size.efsmetered`

Ensure that this folder is at the following location:
`${GOPATH}/github.com/jsalcedo09`

## Getting Started with Efsbeat

### Requirements

* [Golang](https://golang.org/dl/) 1.7

### AWS EFS Storage Beat specific configuration options

- `efsbeat.period`  : (mandatory) The period at which the folders will be size metered.  
- `efsbeat.paths` : (mandatory) A list of paths to be metered by the beat, you can add * to fetch multiple folders in one path, as an example `["/data/*","/data/","/tmp/"]` will create on event with /data/folder1, /data/folder2, /data and /tmp folders disk usage splited in two, `size.real` meaning what operating system see and `size.efsmetered` for what AWS EFS will meter and bill.
- `efsbeat.dironly` : (mandatory) `true` or `false` value, when false, an event is created when using * on path, so for example, `efsbeat.dironly: false` on `/data/*` will result on multiple events for: /data/folder1, /data/folder2, /data/folder1, /data/file1 and /data/file2. *Note:* General folder size calculations are always including files and folders no matter this option

### Init Project
To get running with Efsbeat and also install the
dependencies, run the following command:

```
make setup
```

It will create a clean git history for each major step. Note that you can always rewrite the history if you wish before pushing your changes.

To push Efsbeat in the git repository, run the following commands:

```
git remote set-url origin https://github.com/jsalcedo09/efsbeat
git push origin master
```

For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).

### Build

To build the binary for Efsbeat run the command below. This will generate a binary
in the same directory with the name efsbeat.

```
make
```


### Run

To run Efsbeat with debugging output enabled, run:

```
./efsbeat -c efsbeat.yml -e -d "*"
```


### Test

To test Efsbeat, run the following command:

```
make testsuite
```

alternatively:
```
make unit-tests
make system-tests
make integration-tests
make coverage-report
```

The test coverage is reported in the folder `./build/coverage/`

### Update

Each beat has a template for the mapping in elasticsearch and a documentation for the fields
which is automatically generated based on `etc/fields.yml`.
To generate etc/efsbeat.template.json and etc/efsbeat.asciidoc

```
make update
```


### Cleanup

To clean  Efsbeat source code, run the following commands:

```
make fmt
make simplify
```

To clean up the build directory and generated artifacts, run:

```
make clean
```


### Clone

To clone Efsbeat from the git repository, run the following commands:

```
mkdir -p ${GOPATH}/github.com/jsalcedo09
cd ${GOPATH}/github.com/jsalcedo09
git clone https://github.com/jsalcedo09/efsbeat
```


For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).


## Packaging

The beat frameworks provides tools to crosscompile and package your beat for different platforms. This requires [docker](https://www.docker.com/) and vendoring as described above. To build packages of your beat, run the following command:

```
make package
```

This will fetch and create all images required for the build process. The hole process to finish can take several minutes.
