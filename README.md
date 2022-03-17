# Linked Data Event Stream snapshots package

[![npm](https://img.shields.io/npm/v/@treecg/ldes-snapshot)](https://www.npmjs.com/package/@treecg/ldes-snapshot)

This package facilitates creating a **snapshot** of a **versioned** Linked Data Event Stream ([LDES](https://semiceu.github.io/LinkedDataEventStreams/)). 

## What is a snapshot

A snapshot of an LDES at **time t** is an LDES where only the most recent versions until time **t** are present.

This indicates that for each version, only one remains in the snapshot.

One of the **benefits** of creating a snapshot is that it captures the total state of all members at time t while being **smaller in size**.

A downside, however, is that by throwing away all older versions, it loses the history of that LDES.

### Version materialized snapshot

This package also supports to create the **[version materialization](https://semiceu.github.io/LinkedDataEventStreams/#version-materializations)** of a snapshot.

Note: This package uses [@treecg/version-materialize-rdf.js](https://github.com/TREEcg/version-materialize-rdf.js) as a basis for the version materializations.

## How to create a snapshot

First, you have to install the package.

```bash
npm install @treecg/ldes-snapshot
```

To create a snapshot of an LDES, there are two options.

The first option is to create a snapshot on a N3 Store. This way of creating a snapshot is straightforward. Load the ldes in a Store and then create the snapshot. As simple as that!
However, there is a downside: This approach will only work for an LDES that can be completely be loaded into memory. Which means it only works for small LDESs.

The second option is to create a snapshot streamingwise using a [Transform](https://nodejs.org/api/stream.html#class-streamtransform).
As input a stream of [members](https://github.com/TREEcg/types/blob/main/lib/Member.ts) is required. This stream is then piped through the transformer which will, when the stream stops, emit a stream of snapshot members (which are version materialized).
While working with streams might be a little more difficult, it allows to create a snapshot of a bigger LDESs as an LDES does not have to be loaded in memory. 

### 🔧 Configuring the snapshot

Configuration for creating a snapshot is done by giving an `options` object (which has the [`ISnapshotOptions`](https://github.com/woutslabbinck/LDES-Snapshot/blob/main/src/SnapshotTransform.ts) interface ).

This object has the following parameters:

| parameter name       | default value                 | description                                                  |
| -------------------- | ----------------------------- | ------------------------------------------------------------ |
| `date`               | new Date()                    | A JavaScript [Date](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date) object; the snapshot will be created until this timestamp |
| `snapshotIdentifier` | `http://example.org/snapshot` | The identifier of the snapshot                               |
| `ldesIdentifier`     |                               | The identifier of the LDES of which you want to create a snapshot from (always **required**) |
| `versionOfPath`      |                               | The `ldes:versionOfPath` of the LDES (which is **required** in the Transform) |
| `timeStampPath`      |                               | The `ldes:timestampPath` of the LDES (which is **required** in the Transform) |
| `materialized`       | false                         | When true, the snapshot will be materialized                 |

## Creating a snapshot using an N3 Store

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
const snapshotCreated = await snapshot.create({
    date: new Date("2020-10-05T12:00:00Z"),
    ldesIdentifier: "http://example.org/ES1",
    materialized: true
})
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
<A> <http://purl.org/dc/terms/created> "2020-10-05T11:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .

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
<A> dct:created "2020-10-05T11:00:00"^^xsd:dateTime .
```

## Creating a snapshot using streams

```javascript
const SnapshotTransform = require('@treecg/ldes-snapshot').SnapshotTransform
const Readable = require("stream").Readable

// Note: uses the store defined in the first option of creating a snapshot
const members = store.getObjects(null, 'https://w3id.org/tree#member', null)
const memberStream = new Readable({
    objectMode: true,
    read() {
        for (let member of members) {
            this.push({
                id: member,
                quads: store.getQuads(member, null, null, null)
            })
        }
        this.push(null)
    }
})

const snapshotOptions = {
    date: new Date(),
    ldesIdentifier: "http://example.org/ES1",
    snapshotIdentifier: "http://example.org/snapshot",
    versionOfPath: "http://purl.org/dc/terms/isVersionOf",
    timestampPath: "http://purl.org/dc/terms/created",
    materialized: true
}
const snapshotTransformer = new SnapshotTransform(snapshotOptions)
const memberStreamTransformed = memberStream.pipe(snapshotTransformer)

const Writer = require("n3").Writer
const writer = new Writer();

memberStreamTransformed.on('metadata', quads => {
console.log('metadata')
console.log(writer.quadsToString(quads))
})

memberStreamTransformed.on('data', ({id, quads}) => {
console.log(`member: ${id.value}`)
console.log(writer.quadsToString(quads))
})

memberStreamTransformed.on('end', () => {
console.log('done')
})
```

Which will output the following

```bash
metadata
<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES1> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "2022-03-09T11:47:10.870Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .

member: A
<A> <http://purl.org/dc/terms/hasVersion> <n3-1> .
<A> <http://purl.org/dc/terms/created> "2020-10-06T13:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<A> <https://www.w3.org/2002/07/owl#versionInfo> "v0.0.2" .
<A> <http://www.w3.org/2000/01/rdf-schema#label> "A v0.0.2" .

done
```

## Feedback and questions

Do not hesitate to [report a bug](https://github.com/TREEcg/LDES-Snapshot/issues).

Further questions can also be asked to [Wout Slabbinck](mailto:wout.slabbinck@ugent.be) (developer and maintainer of this repository).
