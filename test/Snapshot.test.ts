import {storeToString, turtleStringToStore} from "../src/util/Conversion";
import {Snapshot} from "../src/Snapshot";
import {DataFactory, Literal, Store} from "n3";
import {ISnapshotOptions} from "../src/SnapshotTransform";
import {DCT, LDES, RDF, TREE} from "../src/util/Vocabularies";
import {dateToLiteral, extractDateFromLiteral} from "../src/util/TimestampUtil";
import quad = DataFactory.quad;
import {SnapshotMetadataParser} from "../src/metadata/SnapshotMetadataParser";
import "jest-rdf"
import {combineSnapshots} from "../src/util/SnapshotUtil";
import namedNode = DataFactory.namedNode;

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
    let snapshotOptions: ISnapshotOptions

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
    describe('materialized', () => {
        beforeEach(() => {
            snapshotOptions = {...snapshotOptions, materialized: true}
        })

        it('is generated as defined by the spec on an LDES', async () => {
            const snapshotStore = await snapshotExample.create(snapshotOptions)
            const snapshotIdentifier = snapshotOptions.snapshotIdentifier!
            const ldesIdentifier = snapshotOptions.ldesIdentifier
            const versionObjectIdentifier = 'http://example.org/resource1'

            expect(snapshotStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, TREE.member, versionObjectIdentifier, null).length).toBe(1)

            expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.hasVersion, 'http://example.org/resource1v1', null).length).toBe(1)
            expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.issued, null, null).length).toBe(1)
            expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.title, null, null).length).toBe(1)
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
            snapshotOptions.ldesIdentifier = "http://example.org/ES1"
            snapshotOptions.timestampPath = "http://purl.org/dc/terms/created"
            const snapshotStore = await snapshot.create(snapshotOptions)

            const snapshotIdentifier = snapshotOptions.snapshotIdentifier!
            const ldesIdentifier = snapshotOptions.ldesIdentifier
            const versionObjectIdentifier = 'A'

            expect(snapshotStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            expect(snapshotStore.getQuads(snapshotIdentifier, TREE.member, versionObjectIdentifier, null).length).toBe(1)

            expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.hasVersion, null, null).length).toBe(1)
            expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.created, null, null).length).toBe(1)
        })
        it('a snapshot with correct members based on date', async () => {
            // Date before the first member
            const dateBefore = new Date('2021-12-15T09:00:00.000Z')
            snapshotOptions.date = dateBefore
            const snapshotStoreBefore = await snapshotExample.create(snapshotOptions)

            const snapshotIdentifier = snapshotOptions.snapshotIdentifier!
            const ldesIdentifier = snapshotOptions.ldesIdentifier
            const versionObjectIdentifier = 'http://example.org/resource1'

            expect(snapshotStoreBefore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotStoreBefore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotStoreBefore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            expect(snapshotStoreBefore.getQuads(snapshotIdentifier, TREE.member, null, null).length).toBe(0)

            const dateLiteralBefore = snapshotStoreBefore.getObjects(snapshotIdentifier, LDES.versionMaterializationUntil, null)[0] as Literal
            expect(extractDateFromLiteral(dateLiteralBefore)).toStrictEqual(dateBefore)

            // date exact at first member added to LDES
            const dateExactFirst = new Date('2021-12-15T10:00:00.000Z')
            snapshotOptions.date = dateExactFirst

            const snapshotStoreExactFirst = await snapshotExample.create(snapshotOptions)

            expect(snapshotStoreExactFirst.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotStoreExactFirst.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotStoreExactFirst.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            expect(snapshotStoreExactFirst.getQuads(snapshotIdentifier, TREE.member, null, null).length).toBe(1)

            const dateLiteralExactFirst = snapshotStoreExactFirst.getObjects(snapshotIdentifier, LDES.versionMaterializationUntil, null)[0] as Literal
            expect(extractDateFromLiteral(dateLiteralExactFirst)).toStrictEqual(dateExactFirst)
            expect(snapshotStoreExactFirst.getObjects(versionObjectIdentifier, DCT.title, null)[0].value).toStrictEqual('First version of the title')

            // date one second after first member
            const dateAfterFirst = new Date('2021-12-15T10:00:01.000Z')
            snapshotOptions.date = dateAfterFirst

            const snapshotStoreAfterFirst = await snapshotExample.create(snapshotOptions)
            const materializedldesAfterFirst =
                `<http://example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/tree#Collection> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationOf> <http://example.org/ES> .
<http://example.org/snapshot> <https://w3id.org/ldes#versionMaterializationUntil> "${dateAfterFirst.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/snapshot> <https://w3id.org/tree#member> <http://example.org/resource1> .
<http://example.org/resource1> <http://purl.org/dc/terms/hasVersion> <http://example.org/resource1v0> .
<http://example.org/resource1> <http://purl.org/dc/terms/issued> "2021-12-15T10:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/resource1> <http://purl.org/dc/terms/title> "First version of the title" .
`
            expect(snapshotStoreAfterFirst.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotStoreAfterFirst.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotStoreAfterFirst.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            expect(snapshotStoreAfterFirst.getQuads(snapshotIdentifier, TREE.member, null, null).length).toBe(1)

            const dateLiteralAfterFirst = snapshotStoreAfterFirst.getObjects(snapshotIdentifier, LDES.versionMaterializationUntil, null)[0] as Literal
            expect(extractDateFromLiteral(dateLiteralAfterFirst)).toStrictEqual(dateAfterFirst)
            expect(snapshotStoreExactFirst.getObjects(versionObjectIdentifier, DCT.title, null)[0].value).toStrictEqual('First version of the title')

            // date after already tested by first two tests
        })
    })

    it('is generated as defined by the spec on an LDES', async () => {
        const snapshotStore = await snapshotExample.create(snapshotOptions)
        const snapshotIdentifier = snapshotOptions.snapshotIdentifier!
        const ldesIdentifier = snapshotOptions.ldesIdentifier
        const versionObjectIdentifier = 'http://example.org/resource1v1'

        expect(snapshotStore.getQuads(snapshotIdentifier, RDF.type, LDES.EventStream, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionOfPath, snapshotOptions.versionOfPath!, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.timestampPath, snapshotOptions.timestampPath!, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, TREE.member, versionObjectIdentifier, null).length).toBe(1)

        expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.isVersionOf, 'http://example.org/resource1', null).length).toBe(1)
        expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.issued, null, null).length).toBe(1)
        expect(snapshotStore.getQuads(versionObjectIdentifier, DCT.title, null, null).length).toBe(1)
    })

    it("generated using a custom identifier for the new tree:collection at current time.", async () => {
        const snapshotIdentifier = 'https://snapshot.ldes/'
        snapshotOptions.snapshotIdentifier = snapshotIdentifier
        const snapshotStore = await snapshotExample.create(snapshotOptions)

        expect(snapshotStore.getQuads(snapshotIdentifier, RDF.type, LDES.EventStream, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionOfPath, snapshotOptions.versionOfPath!, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.timestampPath, snapshotOptions.timestampPath!, null).length).toBe(1)
    })

    it("generated using default values for date, snapshotIdentifier, timestampPath and versionOfPath", async () => {
        const snapshotStore = await snapshotExample.create({
            ldesIdentifier: snapshotOptions.ldesIdentifier
        })
        const snapshotIdentifier = 'http://example.org/snapshot'

        expect(snapshotStore.getQuads(snapshotIdentifier, RDF.type, LDES.EventStream, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.versionOfPath, snapshotOptions.versionOfPath!, null).length).toBe(1)
        expect(snapshotStore.getQuads(snapshotIdentifier, LDES.timestampPath, snapshotOptions.timestampPath!, null).length).toBe(1)
    })
    describe('(incremental)', () => {
        let incrementalStore: Store
        let baseSnapshotStore: Store
        beforeEach(async () => {
            incrementalStore = new Store()
            const date = new Date('2022-01-01')
            incrementalStore.addQuads([
                quad(namedNode("http://example.org/ES"), RDF.terms.type, LDES.terms.EventStream),
                quad(namedNode("http://example.org/ES"), LDES.terms.timestampPath, DCT.terms.issued),
                quad(namedNode("http://example.org/ES"), LDES.terms.versionOfPath, DCT.terms.isVersionOf),
                quad(namedNode("http://example.org/ES"), TREE.terms.member, namedNode("http://example.org/resource2v0")),
                quad(namedNode("http://example.org/ES"), TREE.terms.member, namedNode("http://example.org/resource1v2")),
                quad(namedNode("http://example.org/resource2v0"), namedNode(DCT.title), namedNode('"First version of the title".')),
                quad(namedNode("http://example.org/resource2v0"), namedNode(DCT.isVersionOf), namedNode('http://example.org/resource2')),
                quad(namedNode("http://example.org/resource2v0"), namedNode(DCT.issued), dateToLiteral(date)),
                quad(namedNode("http://example.org/resource1v2"), namedNode(DCT.title), namedNode('"some update".')),
                quad(namedNode("http://example.org/resource1v2"), namedNode(DCT.isVersionOf), namedNode('http://example.org/resource1')),
                quad(namedNode("http://example.org/resource1v2"), namedNode(DCT.issued), dateToLiteral(date))
            ])
            baseSnapshotStore = await snapshotExample.create({
                ldesIdentifier: snapshotOptions.ldesIdentifier
            })
        });

        it('throws an error when it is not about the same LDES.', async () => {
            const baseSnapshotMetadata = SnapshotMetadataParser.extractSnapshotMetadata(baseSnapshotStore)
            baseSnapshotMetadata.snapshotOf = "test"
            const newSnapshot = new Snapshot(incrementalStore)
            await expect(newSnapshot.create({
                ldesIdentifier: snapshotOptions.ldesIdentifier
            }, baseSnapshotMetadata.getStore())).rejects.toThrow(Error)
        });

        it('creates an incremental snapshot by overwriting everything from the original.', async () => {
            const date = new Date()
            const options = {
                ldesIdentifier: snapshotOptions.ldesIdentifier,
                date
            }
            const newSnapshot = new Snapshot(incrementalStore)
            const newSnapshotStore = await newSnapshot.create(options, baseSnapshotStore)

            const incrementalSnapshot = await new Snapshot(incrementalStore).create(options)
            expect(newSnapshotStore).toBeRdfIsomorphic(await combineSnapshots(incrementalSnapshot, baseSnapshotStore))
        });

        it('creates an incremental snapshot by adding a new resource in the incremental store.', async () => {
            const date = new Date()
            const options = {
                ldesIdentifier: snapshotOptions.ldesIdentifier,
                date
            }
            incrementalStore.removeQuad(quad(namedNode("http://example.org/ES"), TREE.terms.member, namedNode("http://example.org/resource1v2")))

            const newSnapshot = new Snapshot(incrementalStore)
            const newSnapshotStore = await newSnapshot.create(options, baseSnapshotStore)

            const incrementalSnapshot = await new Snapshot(incrementalStore).create(options)
            expect(newSnapshotStore).toBeRdfIsomorphic(await combineSnapshots(incrementalSnapshot, baseSnapshotStore))
        });
    });

})
