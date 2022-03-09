import {turtleStringToStore} from "../../src/util/Conversion";
import {
    createSnapshotMetadata,
    extractSnapshotOptions,
    retrieveTimestampProperty,
    retrieveVersionOfProperty
} from "../../src/util/SnapshotUtil";
import {DCT, LDES, RDF, TREE} from "../../src/util/Vocabularies";
import {DataFactory, Literal, Store} from "n3";
import namedNode = DataFactory.namedNode;
import {extractDateFromLiteral} from "../../src/util/TimestampUtil";

describe("A SnapshotUtil", () => {
    const ldesIdentifier = 'http://example.org/ES'
    let store: Store
    beforeAll(async () => {
        const ldes = `
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
        store = await turtleStringToStore(ldes)
    })
    describe("for retrieving the ldes:versionOfPath", () => {
        it("errors when there is no versionOfPath predicate found in the store", async () => {
            const ldesString = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix ex: <http://example.org/> .

ex:ES a ldes:EventStream;
    ldes:timestampPath dct:issued.`
            const store = await turtleStringToStore(ldesString)
            expect(() => retrieveVersionOfProperty(store, ldesIdentifier)).toThrow(Error)
        })

        it("errors when there is no versionOfPath predicate found in the store for the given identifier", async () => {
            expect(() => retrieveVersionOfProperty(store, 'http://example.org/identifier')).toThrow(Error)
        })

        it("retrieves the correct versionOfPath", async () => {
            expect(retrieveVersionOfProperty(store, ldesIdentifier)).toBe(DCT.isVersionOf)
        })
    })
    describe("for retrieving the ldes:timestampPath", () => {
        it("errors when there is no timestampPath predicate found in the store", async () => {
            const ldesString = `
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix ex: <http://example.org/> .

ex:ES a ldes:EventStream;
    ldes:versionOfPath dct:isVersionOf.`
            const store = await turtleStringToStore(ldesString)
            expect(() => retrieveTimestampProperty(store, ldesIdentifier)).toThrow(Error)
        })

        it("errors when there is no timestampPath predicate found in the store for the given identifier", async () => {
            expect(() => retrieveTimestampProperty(store, 'http://example.org/identifier')).toThrow(Error)
        })

        it("retrieves the correct timestampPath", async () => {
            expect(retrieveTimestampProperty(store, ldesIdentifier)).toBe(DCT.issued)
        })
    })
    describe("for extracting Snapshot Options", () => {
        it("returns a ISnapshotOptions object", () => {
            const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)
            expect(snapshotOptions.ldesIdentifier).toBe(ldesIdentifier)
            expect(snapshotOptions.timestampPath).toBe(DCT.issued)
            expect(snapshotOptions.versionOfPath).toBe(DCT.isVersionOf)
            expect(snapshotOptions.date).toBe(undefined)
            expect(snapshotOptions.snapshotIdentifier).toBe(undefined)
        })

    })
    describe("for creating the metadata of a Snapshot", () => {

        it("creates a store that contains the metadata triples of a snapshot", () => {
            const date = new Date()
            const snapshotIdentifier = 'http://example.org/snapshot'
            const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)
            snapshotOptions.snapshotIdentifier = snapshotIdentifier
            snapshotOptions.date = date
            const snapshotMetadataStore = createSnapshotMetadata(snapshotOptions)

            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)

            const dateLiteral = snapshotMetadataStore.getObjects(snapshotIdentifier, LDES.versionMaterializationUntil, null)[0] as Literal
            expect(extractDateFromLiteral(dateLiteral)).toStrictEqual(date)
        })

        it("creates a store that contains the metadata triples of a snapshot with default settings", () => {

            const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)

            const snapshotMetadataStore = createSnapshotMetadata(snapshotOptions)
            const snapshotIdentifier = `${ldesIdentifier}Snapshot`

            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
        })
    })
})
