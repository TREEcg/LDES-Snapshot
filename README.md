# Linked Data Event Stream snapshots package

This package facilitates creating a **snapshot** of a **versioned** Linked Data Event Stream ([LDES](https://semiceu.github.io/LinkedDataEventStreams/)). 

A snapshot is the **[version materialization](https://semiceu.github.io/LinkedDataEventStreams/#version-materializations)** of a versioned LDES.

Note: This package uses [@treecg/version-materialize-rdf.js](https://github.com/TREEcg/version-materialize-rdf.js) as a basis for the version materializations.

## How to create a snapshot

```bash
npm install @treecg/ldes-snapshot
```

Below is an example of how to use this package. As LDES, the example from [version materialization ](https://semiceu.github.io/LinkedDataEventStreams/#version-materializations)in the LDES specification is used.

```javascript
const Snapshot = require('@treecg/ldes-snapshot').Snapshot;
const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require('streamify-string');
    
const ldesString = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <https://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex: <http://example.org/> .

ex:ES1 a ldes:EventStream;
       ldes:versionOfPath dct:isVersionOf;
       ldes:timestampPath dct:created;
       tree:member [
           dct:isVersionOf <A> ;
           dct:created "2020-10-05T11:00:00Z"^^xsd:dateTime;
           owl:versionInfo "v0.0.1";
           rdfs:label "A v0.0.1"
       ], [
           dct:isVersionOf <A> ;
           dct:created "2020-10-06T13:00:00Z"^^xsd:dateTime;
           owl:versionInfo "v0.0.2";
           rdfs:label "A v0.0.2"
       ].`
// have the above ldes as a N3.Store using rdf-parse.js (https://github.com/rubensworks/rdf-parse.js)
const textStream = streamifyString(ldesString);
const quadStream = rdfParser.parse(textStream, {contentType: 'text/turtle'});
const store = await storeStream(quadStream);

// load the ldes store
const snapshot = new Snapshot(store);
// create the snapshot at a given time
const snapshotCreated = snapshot.create({date: new Date("2020-10-05T12:00:00Z")})
// Note: when no arguments are given, a snapshot is taken using the current time
```

When converting the store back to string, the following output is achieved

```javascript
const Writer = require("n3").Writer
const writer = new Writer();
console.log(writer.quadsToString(snapshotCreated.getQuads()))
```

```turtle
<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES1> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "2020-10-05T12:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <A> .
<A> <http://purl.org/dc/terms/hasVersion> <n3-0> .
<A> <https://www.w3.org/2002/07/owl#versionInfo> "v0.0.1" .
<A> <http://www.w3.org/2000/01/rdf-schema#label> "A v0.0.1" .
<A> <http://purl.org/dc/terms/created> "2020-10-05T11:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .

```

Which is equivalent as the following (when prefixes are added):

```turtle
ex:snapshot <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> tree:Collection .
ex:snapshot ldes:versionMaterializationOf ex:ES1 .
ex:snapshot ldes:versionMaterializationUntil "2020-10-05T12:00:00.000Z"^^xsd:dateTime .
ex:snapshot tree:member <A> .
<A> dct:hasVersion <n3-0> .
<A> owl:versionInfo "v0.0.1" .
<A> rdfs:label "A v0.0.1" .
<A> dct:created "2020-10-05T11:00:00.000Z"^^xsd:dateTime .
```



## Feedback and questions

Do not hesitate to [report a bug](https://github.com/woutslabbinck/LDES-Snapshot/issues).

Further questions can also be asked to [Wout Slabbinck](mailto:wout.slabbinck@ugent.be) (developer and maintainer of this repository).
