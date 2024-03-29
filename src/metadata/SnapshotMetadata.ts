/***************************************
 * Title: SnapshotMetadata
 * Description: Contains the interface and implementation for the metadata of a Snapshot (of a versioned LDES).
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/

import {N3Support} from "./generic/Interfaces";
import {ISnapshotMember} from "./SnapshotMember";
import {DataFactory, Store} from "n3";
import {LDES, RDF, TREE} from "../util/Vocabularies";
import {dateToLiteral} from "../util/TimestampUtil";
import namedNode = DataFactory.namedNode;

/**
 * A Snapshot is a versioned LDES where only the most recent versions until a certain timestamp of a versioned LDES remain
 */
export interface ISnapshot extends N3Support {
    /**
     * Identifier of the snapshot (which in itself is an LDES)
     */
    eventStreamIdentifier: string
    timestampPath: string
    versionOfPath: string
    /**
     * The identifier of the LDES from which the snapshot was created
     */
    snapshotOf: string
    /**
     * Marks the dateTime at which the snapshot was created
     */
    snapshotUntil: Date

    materialized: boolean

    members: ISnapshotMember[]
}

export class SnapshotMetadata implements ISnapshot {
    eventStreamIdentifier: string
    timestampPath: string
    versionOfPath: string
    snapshotOf: string
    snapshotUntil: Date
    members: ISnapshotMember[]
    materialized: boolean

    constructor(args: {
        eventStreamIdentifier: string
        timestampPath: string
        versionOfPath: string
        snapshotOf: string
        snapshotUntil: Date
        members: ISnapshotMember[]
        materialized: boolean

    }) {
        this.eventStreamIdentifier = args.eventStreamIdentifier
        this.timestampPath = args.timestampPath
        this.versionOfPath = args.versionOfPath
        this.snapshotOf = args.snapshotOf
        this.snapshotUntil = args.snapshotUntil
        this.members = args.members
        this.materialized = args.materialized
    }

    public getStore(): Store {
        const store = new Store()
        store.addQuad(namedNode(this.eventStreamIdentifier), RDF.terms.type, LDES.terms.EventStream)
        store.addQuad(namedNode(this.eventStreamIdentifier), LDES.terms.timestampPath, namedNode(this.timestampPath))
        store.addQuad(namedNode(this.eventStreamIdentifier), LDES.terms.versionOfPath, namedNode(this.versionOfPath))
        store.addQuad(namedNode(this.eventStreamIdentifier), LDES.terms.snapshotOf, namedNode(this.snapshotOf))
        store.addQuad(namedNode(this.eventStreamIdentifier), LDES.terms.snapshotUntil, dateToLiteral(this.snapshotUntil))

        this.members.forEach(snapshotMember => {
            store.addQuad(namedNode(this.eventStreamIdentifier), TREE.terms.member, namedNode(snapshotMember.id.value))
            store.addQuads(snapshotMember.quads)
        })
        return store
    }
}
