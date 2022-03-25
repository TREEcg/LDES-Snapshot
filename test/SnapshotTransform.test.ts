import {memberStreamtoStore, storeAsMemberStream, storeToString, turtleStringToStore} from "../src/util/Conversion";
import {Readable} from "stream";
import {ISnapshotOptions, SnapshotTransform} from "../src/SnapshotTransform";
import {extractSnapshotOptions} from "../src/util/SnapshotUtil";
import {DCT, LDES, RDF, TREE} from "../src/util/Vocabularies";
import {DataFactory, Literal, Store} from "n3";
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

    describe("constructor", () => {
        it("errors when no version path is given.", () => {
            const customSnapshotOptions: ISnapshotOptions = {
                ldesIdentifier: snapshotOptions.ldesIdentifier,
                timestampPath: snapshotOptions.timestampPath
            }
            expect(() => new SnapshotTransform(customSnapshotOptions)).toThrow(Error)
        })

        it("errors when no timestamp path is given.", () => {
            const customSnapshotOptions: ISnapshotOptions = {
                ldesIdentifier: snapshotOptions.ldesIdentifier,
                versionOfPath: snapshotOptions.versionOfPath
            }
            expect(() => new SnapshotTransform(customSnapshotOptions)).toThrow(Error)
        })
    })
    it("generates a quad stream for metadata", async () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)

        const test = new Promise((resolve, reject) => {
            memberStreamTransformed.on('end', resolve).on('error', reject)
            memberStreamTransformed.on('metadata', quads => {
                try {
                    expect(quads).toBeInstanceOf(Array)
                    const metadataStore = new Store(quads)
                    expect(metadataStore.getQuads(snapshotIdentifier, RDF.type, LDES.EventStream, null).length).toBe(1)
                    expect(metadataStore.getQuads(snapshotIdentifier, LDES.versionOfPath, snapshotOptions.versionOfPath!, null).length).toBe(1)
                    expect(metadataStore.getQuads(snapshotIdentifier, LDES.timestampPath, snapshotOptions.timestampPath!, null).length).toBe(1)
                } catch (e) {
                    reject(e)
                }
            })
            memberStreamTransformed.on('data', () => {
            })

        })
        await test
    })

    it("generates a data stream of members", async () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        const test = new Promise((resolve, reject) => {
            memberStreamTransformed.on('end', resolve).on('error', reject)
            memberStreamTransformed.on('data', member => {
                try {
                    expect(member.quads).toBeInstanceOf(Array)

                    expect(member.id.value).toBe('http://example.org/resource1v1')
                    const memberStore = new Store(member.quads)

                    expect(memberStore.getQuads(member.id.value, DCT.isVersionOf, 'http://example.org/resource1', null).length).toBe(1)
                    expect(memberStore.getQuads(member.id.value, DCT.issued, null, null).length).toBe(1)
                    expect(memberStore.getQuads(member.id.value, DCT.title, null, null).length).toBe(1)

                    expect(memberStore.getObjects(member.id.value, DCT.title, null)[0].value).toStrictEqual('Title has been updated once')
                    const dateLiteral = memberStore.getObjects(member.id.value, DCT.issued, null)[0] as Literal
                    expect(extractDateFromLiteral(dateLiteral)).toStrictEqual(new Date("2021-12-15T12:00:00.000Z"))
                } catch (e) {
                    reject(e)
                }
            })

        })
        await test
    })

    it("makes it possible the stream of snapshot members to a store", async () => {
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = memberStream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(memberStreamTransformed, snapshotIdentifier)
        const versionObjectIdentifier = 'http://example.org/resource1v1'

        expect(transformedStore.getQuads(versionObjectIdentifier, DCT.isVersionOf, 'http://example.org/resource1', null).length).toBe(1)
        expect(transformedStore.getQuads(versionObjectIdentifier, DCT.issued, null, null).length).toBe(1)
        expect(transformedStore.getQuads(versionObjectIdentifier, DCT.title, null, null).length).toBe(1)

        expect(transformedStore.getQuads(snapshotIdentifier, TREE.member, versionObjectIdentifier, null).length).toBe(1)
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

        expect(transformedStore.getQuads(resourceIdentifier, null, null, null).length).toBe(0)
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

        expect(transformedStore.getQuads(resourceIdentifier, null, null, null).length).toBe(0)
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

        expect(transformedStore.getQuads('http://example.org/resource1v1', null, null, null).length).toBe(3)
    })

    it("handles memberStream with triples in quads that have different subject than member itself", async () => {
        // todo: document more understandable and do actual test
        const stream = new Readable({
            objectMode: true,
            read() {
                const identifierNode = namedNode("http://example.org/ex#v1")
                const versionNode = namedNode("http://example.org/ex")
                store = new Store();
                store.addQuad(identifierNode, namedNode(DCT.title), literal("test"))
                store.addQuad(identifierNode, namedNode(DCT.isVersionOf), versionNode)
                store.addQuad(identifierNode, namedNode(DCT.issued), dateToLiteral(new Date("2021-12-15T10:00:00.000Z")))
                store.addQuad(identifierNode, namedNode('http://extra/'), namedNode("http://example.org/"))
                store.addQuad(namedNode("http://example.org/"), namedNode(DCT.title), literal("test"))
                this.push({
                    id: identifierNode,
                    quads: store.getQuads(null, null, null, null)
                })
                this.push(null)
            }
        })
        snapshotOptions.materialized = true
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = stream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(memberStreamTransformed)
        console.log(storeToString(transformedStore))
        console.log(transformedStore.countQuads(null,null,null,null))
    })

    it("handles memberStream with triples in quads that have blank node subject linked by member", async () => {
        // todo: document more understandable and do actual test
        const stream = new Readable({
            objectMode: true,
            read() {
                const identifierNode = namedNode("http://example.org/ex#v1")
                const versionNode = namedNode("http://example.org/ex")
                store = new Store();
                store.addQuad(identifierNode, namedNode(DCT.title), literal("test"))
                store.addQuad(identifierNode, namedNode(DCT.isVersionOf), versionNode)
                store.addQuad(identifierNode, namedNode(DCT.issued), dateToLiteral(new Date("2021-12-15T10:00:00.000Z")))
                const bn = store.createBlankNode()
                store.addQuad(identifierNode, namedNode('http://extra/'), bn)
                store.addQuad(bn, namedNode(DCT.title), literal("test"))
                this.push({
                    id: identifierNode,
                    quads: store.getQuads(null, null, null, null)
                })
                this.push(null)
            }
        })
        snapshotOptions.materialized = true
        const snapshotTransformer = new SnapshotTransform(snapshotOptions)
        const memberStreamTransformed = stream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(memberStreamTransformed)
        console.log(storeToString(transformedStore))
        console.log(transformedStore.countQuads(null,null,null,null))
    })
})
