import {memberStreamtoStore, storeAsMemberStream, storeToString, turtleStringToStore} from "../src/util/Conversion";
import {Readable} from "stream";
import {ISnapshotOptions, SnapshotTransform} from "../src/SnapshotTransform";
import {extractSnapshotOptions} from "../src/util/SnapshotUtil";
import {DCT, LDES, RDF, TREE} from "../src/util/Vocabularies";
import {DataFactory, Literal, Store} from "n3";
import * as RDF2 from "@rdfjs/types";
import {dateToLiteral, extractDateFromLiteral} from "../src/util/TimestampUtil";
import quad = DataFactory.quad;
import namedNode = DataFactory.namedNode;
import literal = DataFactory.literal;

describe("A SnapshotTransform", () => {
    const ldesIdentifier = 'http://example.org/ES'
    const snapshotIdentifier = 'http://example.org/snapshot'
    const date = new Date()

    let store: Store
    let memberStream: Readable
    let snapshotOptions: ISnapshotOptions
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
        snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)
        snapshotOptions.date = date
        snapshotOptions.snapshotIdentifier = snapshotIdentifier
    })
    beforeEach(() => {
        memberStream = storeAsMemberStream(store)
    })

    it("generates a quad stream for metadata", () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)

        memberStreamTransformed.on('metadata', quads => {
            expect(quads).toBeInstanceOf(Array)
            const metadataStore = new Store(quads)

            expect(metadataStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
            expect(metadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
            expect(metadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
        })
    })

    it("generates a data stream of members", () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)

        memberStreamTransformed.on('data', member => {
            expect(member.quads).toBeInstanceOf(Array)

            expect(member.id.value).toBe('http://example.org/resource1')
            const memberStore = new Store(member.quads)

            expect(memberStore.getQuads(member.id.value, DCT.hasVersion, 'http://example.org/resource1v1', null).length).toBe(1)
            expect(memberStore.getQuads(member.id.value, DCT.issued, null, null).length).toBe(1)
            expect(memberStore.getQuads(member.id.value, DCT.title, null, null).length).toBe(1)

            expect(memberStore.getObjects(member.id.value, DCT.title, null)[0].value).toStrictEqual('Title has been updated once')
            const dateLiteral = memberStore.getObjects(member.id.value, DCT.issued, null)[0] as Literal
            expect(extractDateFromLiteral(dateLiteral)).toStrictEqual(new Date("2021-12-15T12:00:00.000Z"))
        })
    })
    it("makes it possible the stream of snapshot members to a store", async () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)
        const resourceIdentifier = 'http://example.org/resource1'

        expect(transformedStore.getQuads(resourceIdentifier, DCT.hasVersion, 'http://example.org/resource1v1', null).length).toBe(1)
        expect(transformedStore.getQuads(resourceIdentifier, DCT.issued, null, null).length).toBe(1)
        expect(transformedStore.getQuads(resourceIdentifier, DCT.title, null, null).length).toBe(1)

        expect(transformedStore.getQuads(snapshotIdentifier, TREE.member, resourceIdentifier, null).length).toBe(1)
    })

    it("ignores data in the stream that are not members.", async () => {
        memberStream.push("not a member")
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        // read whole output of the transformed member stream
        await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)
    })

    it("ignores members that have no version ID.", async () => {
        const versionSpecificID = "http://example.org/resource2v0"
        const resourceIdentifier = 'http://example.org/resource2'
        const dateLiteral = dateToLiteral(new Date("2020-10-05T11:00:00Z"))
        memberStream.push({
            id: namedNode(versionSpecificID), quads: [
                quad(namedNode(versionSpecificID), namedNode(DCT.created), dateLiteral),
                quad(namedNode(versionSpecificID), namedNode(DCT.title), literal("some Title.")),
            ]
        })
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        // read whole output of the transformed member stream
        const transformedStore = await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)

        expect(transformedStore.getQuads(resourceIdentifier,null,null,null).length).toBe(0)
    })

    it("ignores members that have no timestamp.", async () => {
        const versionSpecificID = "http://example.org/resource2v0"
        const resourceIdentifier = 'http://example.org/resource2'
        memberStream.push({
            id: namedNode(versionSpecificID), quads: [
                quad(namedNode(versionSpecificID), namedNode(DCT.isVersionOf), namedNode(resourceIdentifier)),
                quad(namedNode(versionSpecificID), namedNode(DCT.title), literal("some Title.")),
            ]
        })
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        // read whole output of the transformed member stream
        const transformedStore = await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)

        expect(transformedStore.getQuads(resourceIdentifier,null,null,null).length).toBe(0)
    })

    it("generated using default values for date and snapshotIdentifier", async () => {
        const snapshotTransformer = new SnapshotTransform({
            ldesIdentifier: snapshotOptions.ldesIdentifier,
            timestampPath: snapshotOptions.timestampPath,
            versionOfPath: snapshotOptions.versionOfPath
        })
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        const snapshotIdentifier = 'http://example.org/snapshot'
        const transformedStore = await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)

        expect(transformedStore.getQuads('http://example.org/resource1',null,null,null).length).toBe(3)
    })
})
