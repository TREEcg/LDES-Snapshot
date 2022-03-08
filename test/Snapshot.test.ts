import {storeToString, turtleStringToStore} from "../src/util/Conversion";
import {Snapshot} from "../src/Snapshotv2";
import {DataFactory} from "n3";
import namedNode = DataFactory.namedNode;
import {ISnapshotOptions} from "../src/SnapshotTransform";
import {DCT} from "../src/util/Vocabularies";

describe("A Snapshot", () => {
    const ldesExample = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .

ex:ES a ldes:EventStream;
    ldes:versionOfPath dct:isVersionOf;
    ldes:timestampPath dct:issued;
    tree:member ex:resource1v0, ex:resource1v1.

ex:resource1v0
    dct:isVersionOf ex:resource1;
    dct:issued "2021-12-15T10:00:00.000Z"^^xsd:dateTime;
    dct:title "First version of the title".

ex:resource1v1
    dct:isVersionOf ex:resource1;
    dct:issued "2021-12-15T12:00:00.000Z"^^xsd:dateTime;
    dct:title "Title has been updated once".
`
    let snapshotExample: Snapshot
    let snapshotOptions :ISnapshotOptions

    beforeAll(async () => {
        const store = await turtleStringToStore(ldesExample)
        snapshotExample = new Snapshot(store)
    })
    beforeEach(async () => {
        snapshotOptions = {
            date: new Date(),
            ldesIdentifier: "http://example.org/ES",
            snapshotIdentifier: "http://example.org/snapshot",
            timestampPath: DCT.issued,
            versionOfPath: DCT.isVersionOf
        }
    })

    it("errors when the LDES in the store has incorrect amount of no versionOfPath properties.", async () => {
        const ldes = `
@prefix ldes: <https://w3id.org/ldes#> .
@prefix dct: <http://purl.org/dc/terms/> .

<ES> a ldes:EventStream;
    ldes:timestampPath dct:issued.
`
        const store = await turtleStringToStore(ldes)
        expect(() => new Snapshot(store)).toThrow(Error)

        const ldes2 = `
@prefix ldes: <https://w3id.org/ldes#> .
@prefix dct: <http://purl.org/dc/terms/> .

<ES> a ldes:EventStream;
    ldes:timestampPath dct:issued, dct:created.
`
        const store2 = await turtleStringToStore(ldes2)
        expect(() => new Snapshot(store2)).toThrow(Error)


    })

    it("errors when the LDES in the store has incorrect amount of no timestampPath properties.", async () => {
        const ldes = `
@prefix ldes: <https://w3id.org/ldes#> .
@prefix dct: <http://purl.org/dc/terms/> .

<ES> a ldes:EventStream;
    ldes:versionOfPath dct:isVersionOf.
`
        const store = await turtleStringToStore(ldes)
        expect(() => new Snapshot(store)).toThrow(Error)

        const ldes2 = `
@prefix ldes: <https://w3id.org/ldes#> .
@prefix dct: <http://purl.org/dc/terms/> .

<ES> a ldes:EventStream;
    ldes:versionOfPath dct:isVersionOf, dct:hasVersion.
`
        const store2 = await turtleStringToStore(ldes2)
        expect(() => new Snapshot(store2)).toThrow(Error)


    })

    it("errors to create snapshot when two LDESes are present", async () => {
        const ldes = `
@prefix ldes: <https://w3id.org/ldes#> .
@prefix dct: <http://purl.org/dc/terms/> .

<ES> a ldes:EventStream;
    ldes:timestampPath dct:issued;
    ldes:versionOfPath dct:isVersionOf.
    
<ES1> a ldes:EventStream;
   ldes:versionOfPath dct:isVersionOf;
   ldes:timestampPath dct:created.
`
        const store = await turtleStringToStore(ldes)
        expect(() => new Snapshot(store)).toThrow(Error)
    })

    it("errors if any materialized members do not have the timestamp property", async () => {
        const ldes = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .

ex:ES a ldes:EventStream;
    ldes:versionOfPath dct:isVersionOf;
    ldes:timestampPath dct:issued;
    tree:member ex:resource1v0.

ex:resource1v0
    dct:isVersionOf ex:resource1;
    dct:title "First version of the title".
`
        const store = await turtleStringToStore(ldes)
        const snapshot = new Snapshot(store)
        expect(() => snapshot.create(snapshotOptions)).toThrow(Error)
    })

/*    it("can materialize an ldes with different options", async () => {
        const options = {
            "versionOfProperty": namedNode('http://purl.org/dc/terms/isVersionOf'), // defaults to dcterms:isVersionOf
            "timestampProperty": namedNode('http://purl.org/dc/terms/created'), // defaults to dcterms:created, but there may be good reasons to change this to e.g., prov:generatedAtTime
            "addRdfStreamProcessingTriple": true
        };
        const store = await turtleStringToStore(ldesExample)
        const snapshot = new Snapshot(store)
        expect(snapshot.materialize(options)).toBeDefined()
    })*/

    describe('creates', () => {
        it('a snapshot as defined by the spec on an LDES', async () => {
            const date = snapshotOptions.date!
            const snapshotStore = await snapshotExample.create(snapshotOptions)
            const materializedldes =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${date.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <http://example.org/resource1> .
<http://example.org/resource1> <http://purl.org/dc/terms/hasVersion> <http://example.org/resource1v1> .
<http://example.org/resource1> <http://purl.org/dc/terms/issued> "2021-12-15T12:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/resource1> <http://purl.org/dc/terms/title> "Title has been updated once" .
`
            expect(storeToString(snapshotStore)).toBe(materializedldes)
        })

        it('a snapshot as defined by the spec on an LDES with blank node members', async () => {
            const ldes = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .
@prefix owl: <https://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

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
            const store = await turtleStringToStore(ldes)
            const snapshot = new Snapshot(store)
            const date = snapshotOptions.date!
            snapshotOptions.ldesIdentifier = "http://example.org/ES1"
            snapshotOptions.timestampPath = "http://purl.org/dc/terms/created"
            const snapshotStore = await snapshot.create(snapshotOptions)
            const materializedldes =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES1> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${date.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <A> .
<A> <http://purl.org/dc/terms/hasVersion> <n3-1> .
<A> <http://purl.org/dc/terms/created> "2020-10-06T13:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<A> <https://www.w3.org/2002/07/owl#versionInfo> "v0.0.2" .
<A> <http://www.w3.org/2000/01/rdf-schema#label> "A v0.0.2" .
`
            expect(storeToString(snapshotStore)).toBe(materializedldes)
        })
        it('a snapshot with correct members based on date', async () => {
            // Date before the first member
            const dateBefore = new Date('2021-12-15T09:00:00.000Z')
            snapshotOptions.date = dateBefore
            const snapshotStoreBefore = await snapshotExample.create(snapshotOptions)
            const materializedldesBefore =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${dateBefore.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
`
            expect(storeToString(snapshotStoreBefore)).toBe(materializedldesBefore)

            // date exact at first member added to LDES
            const dateExactFirst = new Date('2021-12-15T10:00:00.000Z')
            snapshotOptions.date = dateExactFirst

            const snapshotStoreExactFirst = await snapshotExample.create(snapshotOptions)
            const materializedldesExactFirst =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${dateExactFirst.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <http://example.org/resource1> .
<http://example.org/resource1> <http://purl.org/dc/terms/hasVersion> <http://example.org/resource1v0> .
<http://example.org/resource1> <http://purl.org/dc/terms/issued> "2021-12-15T10:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/resource1> <http://purl.org/dc/terms/title> "First version of the title" .
`
            expect(storeToString(snapshotStoreExactFirst)).toBe(materializedldesExactFirst)

            // date one second after first member
            const dateAfterFirst = new Date('2021-12-15T10:00:01.000Z')
            snapshotOptions.date = dateAfterFirst

            const snapshotStoreAfterFirst =  await snapshotExample.create(snapshotOptions)
            const materializedldesAfterFirst =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${dateAfterFirst.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <http://example.org/resource1> .
<http://example.org/resource1> <http://purl.org/dc/terms/hasVersion> <http://example.org/resource1v0> .
<http://example.org/resource1> <http://purl.org/dc/terms/issued> "2021-12-15T10:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/resource1> <http://purl.org/dc/terms/title> "First version of the title" .
`
            expect(storeToString(snapshotStoreAfterFirst)).toBe(materializedldesAfterFirst)
            // date after already tested by first 2 tests in Create
        })
        it("a snapshot using a custom identifier for the new tree:collection at current time.", async () => {
            const snapshotIdentifier = 'https://snapshot.ldes/'
            snapshotOptions.snapshotIdentifier = snapshotIdentifier
            const snapshotStore =  await snapshotExample.create(snapshotOptions)
            expect(storeToString(snapshotStore)).toContain(snapshotIdentifier)
        })
    })
})
