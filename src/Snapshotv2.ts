import {DataFactory, Literal, Quad, Store} from "n3";
import {IMaterializeOptions, materialize} from "@treecg/version-materialize-rdf.js";
import namedNode = DataFactory.namedNode;
import {DCT, LDES, RDF, TREE} from "./util/Vocabularies";
import quad = DataFactory.quad;
import {dateToLiteral, extractTimestampFromLiteral, timestampToLiteral} from "./util/TimestampUtil";
import {
    NamedNode
} from "@rdfjs/types";
import {createSnapshotMetadata} from "./util/snapshotUtil";
import {ISnapshotOptions, SnapshotTransform} from "./SnapshotTransform";
import {memberStreamtoStore, storeAsMemberStream, storeToString} from "./util/Conversion";

/***************************************
 * Title: Snapshot
 * Description: Class to create a materialized LDES at a certain timestamp (only works for small LDESes)
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 03/03/2022
 *****************************************/
export class Snapshot {
    private baseStore: Store;
    private readonly ldesIRI: string; //todo: maybe remove?

    constructor(store: Store) {
        this.baseStore = store;
        this.ldesIRI = this.retrieveLdesIRI()
    }

    private retrieveLdesIRI(): string {
        const possibleIdentifiers = this.baseStore.getSubjects(RDF.type, LDES.EventStream, null)
        if (possibleIdentifiers.length !== 1) {
            throw Error(`Found ${possibleIdentifiers.length} LDESs, only expected one`)
        }
        return possibleIdentifiers[0].id
    }


    /**
     * Creates a snapshot from a version LDES. (see: https://semiceu.github.io/LinkedDataEventStreams/#version-materializations)
     * Default:
     * uses "http://example.org/snapshot" as identifier for the snapshot (tree:collection)
     * and uses the current time for ldes:versionMaterializationUntil
     * @param options optional extra paramaters for creating the snapshot
     * @return {Promise<Store>}
     */
    async create(options: ISnapshotOptions): Promise<Store> {
        options.date = options.date ? options.date : new Date();
        options.snapshotIdentifier = options.snapshotIdentifier ? options.snapshotIdentifier : 'http://example.org/snapshot';


        const snapshotStore = createSnapshotMetadata(options)
        const memberStream = storeAsMemberStream(this.baseStore)
        const snapshotTransformer = new SnapshotTransform(options);
        const transformationOutput = memberStream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(transformationOutput, options.snapshotIdentifier)
        snapshotStore.addQuads(transformedStore.getQuads(null,null,null,null))

        return snapshotStore
    }

}


// copied from https://stackoverflow.com/a/43467144
function isValidHttpUrl(string: string): boolean {
    let url;

    try {
        url = new URL(string);
    } catch (_) {
        return false;
    }

    return url.protocol === "http:" || url.protocol === "https:";
}
