/***************************************
 * Title: SnapshotMember
 * Description: Contains an extension to Member interface: added objectIdentifier and timestamp
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
import {Member} from "@treecg/types";
import {N3Support} from "./generic/Interfaces";
import {Quad, Term} from "@rdfjs/types";
import {DataFactory, Store} from "n3";
import namedNode = DataFactory.namedNode;

/**
 * an extension of {@link Member}
 *
 * When the snapshot is materialized, the objectIdentifier is the same as the id
 */
export interface ISnapshotMember extends Member, N3Support {
    objectIdentifier: string
    timestamp: Date
}

export class SnapshotMember implements ISnapshotMember {
    objectIdentifier: string;
    timestamp: Date;
    id: Term;
    quads: Quad[];

    constructor(quads: Quad[], memberId: string, objectID: string, timestamp: Date) {
        this.id = namedNode(memberId);
        this.objectIdentifier = objectID
        this.quads = quads
        this.timestamp = timestamp
    }

    public getStore() {
        return new Store(this.quads)
    }
}
