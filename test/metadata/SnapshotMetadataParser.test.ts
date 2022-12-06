import "jest-rdf"
import {DataFactory, Store} from "n3";
import {storeToString, turtleStringToStore} from "../../src/util/Conversion";
import {namedNode} from "@rdfjs/data-model";
import {DCT, LDES, RDF, TREE} from "../../src/util/Vocabularies";
import literal = DataFactory.literal;
import {dateToLiteral} from "../../src/util/TimestampUtil";
import {SnapshotMetadataParser} from "../../src/metadata/SnapshotMetadataParser";

function addMember(memberId: string, date: Date, store: Store, snapshotIdentifier: string): void {
    store.addQuad(namedNode(snapshotIdentifier), TREE.terms.member, namedNode(memberId))
    store.addQuad(namedNode(memberId), DCT.terms.title, literal("test"))
    store.addQuad(namedNode(memberId), DCT.terms.issued, dateToLiteral(date))
    store.addQuad(namedNode(memberId), DCT.terms.isVersionOf, namedNode("example.org/" + date.valueOf()))
}

describe('A SnapshotMetadataParser', () => {
    let store: Store;
    const date = new Date()
    const snapshotIdentifier = "http//example.org/snapshot"
    const originalLdes = "http://example.org/ES"
    const snapshotOfNode = DCT.terms.issued

    beforeEach(async () => {
        store = await turtleStringToStore(`
<http//example.org/snapshot> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/ldes#EventStream> .
<http//example.org/snapshot> <https://w3id.org/ldes#timestampPath> <http://purl.org/dc/terms/issued> .
<http//example.org/snapshot> <https://w3id.org/ldes#versionOfPath> <http://purl.org/dc/terms/isVersionOf> .
<http//example.org/snapshot> <https://w3id.org/ldes#snapshotOf> <http://example.org/ES> .
<http//example.org/snapshot> <https://w3id.org/ldes#snapshotUntil> "${date.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        `)
    });

    it('fails parsing when there is no snapshotOf predicate.', () => {
        store.removeQuad(namedNode(snapshotIdentifier), LDES.terms.snapshotOf, namedNode(originalLdes))
        expect(() => SnapshotMetadataParser.extractSnapshotMetadata(store)).toThrow(Error)
    });

    it('fails parsing when there is no snapshotUntil predicate.', () => {
        store.removeQuad(namedNode(snapshotIdentifier), LDES.terms.snapshotUntil, dateToLiteral(date))
        expect(() => SnapshotMetadataParser.extractSnapshotMetadata(store)).toThrow(Error)
    });

    it('can parse snapshot Metadata which does not have any members.', () => {
        let snapshotMetadata = SnapshotMetadataParser.extractSnapshotMetadata(store)
        expect(snapshotMetadata.getStore()).toBeRdfIsomorphic(store)
    });

    it('can parse snapshot Metadata with multiple members.', () => {
        addMember("example.org/member1", new Date("2022-01-01"), store, snapshotIdentifier)
        addMember("example.org/member2", new Date("2022-05-01"), store, snapshotIdentifier)
        let snapshotMetadata = SnapshotMetadataParser.extractSnapshotMetadata(store)
        expect(snapshotMetadata.getStore()).toBeRdfIsomorphic(store)
    })

    it('fails parsing a member does not have a versionOfPath.', () => {
        const memberNode = namedNode("example.org/member")
        store.addQuad(namedNode(snapshotIdentifier), TREE.terms.member, memberNode)
        store.addQuad(memberNode, DCT.terms.issued, dateToLiteral(new Date()))
        expect(() => SnapshotMetadataParser.extractSnapshotMetadata(store)).toThrow(Error)
    });

    it('fails parsing a member does not have a timestampPath.', () => {
        const memberNode = namedNode("example.org/member")
        store.addQuad(namedNode(snapshotIdentifier), TREE.terms.member, memberNode)
        store.addQuad(memberNode, DCT.terms.isVersionOf, namedNode("example.org/" + date.valueOf()))
        expect(() => SnapshotMetadataParser.extractSnapshotMetadata(store)).toThrow(Error)
    });

    it('fails parsing when there are multiple LDESes.', () => {
        store.addQuad(namedNode("test"), RDF.terms.type, LDES.terms.EventStream)
        expect(() => SnapshotMetadataParser.extractSnapshotMetadata(store)).toThrow(Error)
    });

    it('can parse a snapshot given its LDES identifier.', () => {
        const quads = store.getQuads(null,null,null,null)
        store.addQuad(namedNode("test"), RDF.terms.type, LDES.terms.EventStream)
        let snapshotMetadata = SnapshotMetadataParser.extractSnapshotMetadata(store, snapshotIdentifier)
        expect( snapshotMetadata.getStore()).toBeRdfIsomorphic(quads)
    });

});
