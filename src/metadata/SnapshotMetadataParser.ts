import {AbstractMetadataParser} from "./AbstractMetadataParser";
import {Literal, Store} from "n3";
import {ISnapshot, SnapshotMetadata} from "./SnapshotMetadata";
import {extractDateFromLiteral} from "../util/TimestampUtil";
import {LDES} from "../util/Vocabularies";
import {ISnapshotMember, SnapshotMember} from "./SnapshotMember";
import {extractMembers} from "../util/Conversion";

/***************************************
 * Title: SnapshotMetadataParser
 * Description: A class that parses metadata for a Snapshot (https://data.vlaanderen.be/doc/applicatieprofiel/ldes#Snapshot) {@link ISnapshot}
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
export class SnapshotMetadataParser extends AbstractMetadataParser {
    /**
     * Parses an N3 Store to {@link ISnapshot}.
     * Only deals with non materialized snapshots.
     *
     * Note: Currently does not parse the shape of the LDES
     *
     * @param store An N3 Store.
     * @param eventStreamIdentifier (optional) URI of the Linked Data Event Stream Identifier.
     * @returns {SnapshotMetadata}
     */
    public static extractSnapshotMetadata(store: Store, eventStreamIdentifier?: string): ISnapshot {
        eventStreamIdentifier = eventStreamIdentifier ?? this.parseLDESIdentifier(store)
        const timestampPath = this.parseTimestampPath(store, eventStreamIdentifier)
        const versionOfPath = this.parseVersionOfPath(store, eventStreamIdentifier)
        const snapshotOf = this.parseSnapshotOf(store, eventStreamIdentifier)
        const snapshotUntil = this.parseSnapshotUntil(store, eventStreamIdentifier)
        const members = this.extractSnapshotMembers(store, eventStreamIdentifier, timestampPath, versionOfPath)

        const snapshot = new SnapshotMetadata({
            eventStreamIdentifier,
            timestampPath,
            versionOfPath,
            snapshotOf,
            snapshotUntil,
            members,
            materialized: false
        })
        return snapshot
    }

    /**
     * Extracts non materialized {@link ISnapshotMember}s from an N3 Store.
     *
     * @param store An N3 Store.
     * @param ldesIdentifier
     * @param timestampPath The timestampPath of a versioned LDES
     * @param versionOfPath The versionOfPath of a versioned LDES
     * @returns {ISnapshotMember[]}
     */
    protected static extractSnapshotMembers(store: Store, ldesIdentifier: string, timestampPath: string, versionOfPath: string): ISnapshotMember[] {
        const members = extractMembers(store, ldesIdentifier)
        const snapshotMembers: ISnapshotMember[] = []

        members.forEach(member => {
            const memberIdentifier = member.id.value
            const objectIdentifier = extractObjectIdentifier(new Store(member.quads), versionOfPath, memberIdentifier)
            const date = extractDate(new Store(member.quads), timestampPath, memberIdentifier)
            snapshotMembers.push(new SnapshotMember(member.quads, memberIdentifier, objectIdentifier, date))
        });
        return snapshotMembers

        function extractObjectIdentifier(store: Store, versionOfPath: string, memberId: string): string { // should there be a memberId given?
            const ObjectIdentifierNodes = store.getObjects(memberId, versionOfPath, null)
            if (ObjectIdentifierNodes.length !== 1) {
                throw Error(`Expected one versionOfPath per member. ${ObjectIdentifierNodes.length} are present for member ${memberId}.`)
            }
            return ObjectIdentifierNodes[0].value
        }

        function extractDate(store: Store, timestampPath: string, memberId: string): Date { // should there be a memberId given?
            const dateNodes = store.getObjects(memberId, timestampPath, null)
            if (dateNodes.length !== 1) {
                throw Error(`Expected one timestampPath per member. ${dateNodes.length} are present for member ${memberId}.`)
            }
            return extractDateFromLiteral(dateNodes[0] as Literal)
        }
    }

    protected static parseSnapshotOf(store: Store, identifier: string): string {
        const snapshotOfNodes = store.getObjects(identifier, LDES.terms.snapshotOf, null)
        if (snapshotOfNodes.length !== 1) {
            throw Error(`Expected one snapshotOf. ${snapshotOfNodes.length} are present.`)
        }
        return snapshotOfNodes[0].value
    }

    protected static parseSnapshotUntil(store: Store, identifier: string): Date {
        const snapshotUntilNodes = store.getObjects(identifier, LDES.terms.snapshotUntil, null)
        if (snapshotUntilNodes.length !== 1) {
            throw Error(`Expected one snapshotUntil. ${snapshotUntilNodes.length} are present.`)
        }
        return extractDateFromLiteral(snapshotUntilNodes[0] as Literal)
    }
}
