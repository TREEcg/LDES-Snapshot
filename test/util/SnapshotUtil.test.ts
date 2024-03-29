import {turtleStringToStore} from "../../src/util/Conversion";
import {
    combineSnapshots,
    createSnapshotMetadata,
    extractSnapshotOptions,
    isMember,
    retrieveTimestampProperty,
    retrieveVersionOfProperty
} from "../../src/util/SnapshotUtil";
import {DCT, LDES, RDF, TREE} from "../../src/util/Vocabularies";
import {DataFactory, Literal, Store} from "n3";
import {dateToLiteral, extractDateFromLiteral} from "../../src/util/TimestampUtil";
import {ISnapshotOptions} from "../../src/SnapshotTransform";
import {ISnapshot} from "../../src/metadata/SnapshotMetadata";
import {SnapshotMetadataInitializer} from "../../src/metadata/SnapshotMetadataInitializer";
import {SnapshotMetadataParser} from "../../src/metadata/SnapshotMetadataParser";
import {SnapshotMember} from "../../src/metadata/SnapshotMember";
import "jest-rdf"
import namedNode = DataFactory.namedNode;
import quad = DataFactory.quad;
import literal = DataFactory.literal;

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
        describe("materialized", () => {
            it("creates a store that contains the metadata triples of a snapshot", () => {
                const date = new Date()
                const snapshotIdentifier = 'http://example.org/snapshot'
                const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)
                snapshotOptions.snapshotIdentifier = snapshotIdentifier
                snapshotOptions.date = date
                snapshotOptions.materialized = true
                const snapshotMetadataStore = createSnapshotMetadata(snapshotOptions)

                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)

                const dateLiteral = snapshotMetadataStore.getObjects(snapshotIdentifier, LDES.versionMaterializationUntil, null)[0] as Literal
                expect(extractDateFromLiteral(dateLiteral)).toStrictEqual(date)
            })

            it("creates a store that contains the metadata triples of a snapshot with default settings", () => {

                const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)
                snapshotOptions.materialized = true

                const snapshotMetadataStore = createSnapshotMetadata(snapshotOptions)
                const snapshotIdentifier = `${ldesIdentifier}Snapshot`

                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, RDF.type, TREE.Collection, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationOf, ldesIdentifier, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionMaterializationUntil, null, null).length).toBe(1)
            })
        })
        describe("not materialized", () => {
            it("errors when no version path is given.", () => {
                const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)

                const customSnapshotOptions: ISnapshotOptions = {
                    ldesIdentifier: snapshotOptions.ldesIdentifier,
                    timestampPath: snapshotOptions.timestampPath
                }
                expect(() => createSnapshotMetadata(customSnapshotOptions)).toThrow(Error)
            })

            it("errors when no timestamp path is given.", () => {
                const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)

                const customSnapshotOptions: ISnapshotOptions = {
                    ldesIdentifier: snapshotOptions.ldesIdentifier,
                    versionOfPath: snapshotOptions.versionOfPath
                }
                expect(() => createSnapshotMetadata(customSnapshotOptions)).toThrow(Error)
            })

            it("creates a store that contains the metadata triples of a snapshot with default settings", () => {
                const snapshotOptions = extractSnapshotOptions(store, ldesIdentifier)

                const snapshotMetadataStore = createSnapshotMetadata(snapshotOptions)
                const snapshotIdentifier = `${ldesIdentifier}Snapshot`

                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, RDF.type, LDES.EventStream, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.timestampPath, snapshotOptions.timestampPath!, null).length).toBe(1)
                expect(snapshotMetadataStore.getQuads(snapshotIdentifier, LDES.versionOfPath, snapshotOptions.versionOfPath!, null).length).toBe(1)
            })
        })
    })

    describe("for testing whether some data is a member", () => {
        let member: any
        beforeEach(() => {
            member = {
                id: namedNode("http://example.org/resource1"),
                quads: [
                    quad(namedNode("http://example.org/resource1"), namedNode(DCT.title), literal("some title"))
                ]
            }
        })
        it("returns true for a member conforming to the Member interface.", () => {
            expect(isMember(member)).toBeTruthy()
        })
        it("returns false for a string.", () => {
            expect(isMember("data")).toBeFalsy()
        })
        it("returns false when there is no id.", () => {
            member.id = null
            expect(isMember(member)).toBeFalsy()
        })
        it("returns false when the id is a string instead of a Term.", () => {
            member.id = 'something'
            expect(isMember(member)).toBeFalsy()
        })

        it("returns false when there are no quads.", () => {
            member.quads = null
            expect(isMember(member)).toBeFalsy()

        })
        it("returns false when quads is an array of strings.", () => {
            member.quads = ["Something"]
            expect(isMember(member)).toBeFalsy()

        })
        it("returns false when quads is an empty array of strings.", () => {
            member.quads = []
            expect(isMember(member)).toBeFalsy()

        })

    })

    describe('for combining two snapshots', () => {
        let baseSnapshot: ISnapshot
        let incrementalSnapshot: ISnapshot
        let eventStreamIdentifier: string
        const timestampPath = "http://example.org/timestampPath"
        const versionOfPath = "http://example.org/versionOfPath"
        beforeEach(() => {
            eventStreamIdentifier = "http://example.org/#ES"
            baseSnapshot = SnapshotMetadataInitializer.generateSnapshotMetadata({
                ldesIdentifier: eventStreamIdentifier,
                snapshotIdentifier: "http://example.org/#base",
                timestampPath,
                versionOfPath
            })

            incrementalSnapshot = SnapshotMetadataInitializer.generateSnapshotMetadata({
                ldesIdentifier: eventStreamIdentifier,
                snapshotIdentifier: "http://example.org/#incremental",
                timestampPath,
                versionOfPath
            })
        });


        it('throws an error when the two snapshots did not come from the same LDES.', async () => {
            incrementalSnapshot.snapshotOf = "some ES"

            const combined = async () => combineSnapshots(baseSnapshot.getStore(), incrementalSnapshot.getStore())
            await expect(combined()).rejects.toThrow(Error)
        });

        it('succeeds when any of the snapshots have no members.', async () => {
            const combined = await combineSnapshots(baseSnapshot.getStore(), incrementalSnapshot.getStore())
            const combinedMetadata = SnapshotMetadataParser.extractSnapshotMetadata(combined)
            await expect(combinedMetadata).toEqual(incrementalSnapshot)
        });

        it('contains the most recent versions.', async () => {
            const makeSnapshotMember = (memberId: string, objectId: string, date: Date) => {
                let memberNode = namedNode(memberId)
                return new SnapshotMember(
                    [quad(memberNode, namedNode(versionOfPath), namedNode(objectId)),
                        quad(memberNode, namedNode(timestampPath), dateToLiteral(date)),
                        quad(memberNode, namedNode("test"), literal("title"))],
                    memberId,
                    objectId,
                    date
                )
            }
            const member1Id = "http://example.org/member1"
            const member2Id = "http://example.org/member2"
            const member = "http://example.org/member"
            const member1 = makeSnapshotMember(member1Id, member, new Date(0))
            const member2 = makeSnapshotMember(member2Id, member, new Date())

            baseSnapshot.members.push(member1)
            incrementalSnapshot.members.push(member2)

            const combined = await combineSnapshots(baseSnapshot.getStore(), incrementalSnapshot.getStore())
            expect(combined).toBeRdfIsomorphic(incrementalSnapshot.getStore())
        });
    });
})
