import {Store} from "n3";
import {createSnapshotMetadata, retrieveTimestampProperty, retrieveVersionOfProperty} from "./util/SnapshotUtil";
import {ISnapshotOptions, SnapshotTransform} from "./SnapshotTransform";
import {memberStreamtoStore, storeAsMemberStream} from "./util/Conversion";

/***************************************
 * Title: Snapshot
 * Description: Class to create a materialized LDES at a certain timestamp (only works for small LDESes)
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 03/03/2022
 *****************************************/
export class Snapshot {
    private baseStore: Store;

    /**
     *
     * @param store an N3 Store containing a versioned LDES from which you want to create a Snapshot
     */
    constructor(store: Store) {
        this.baseStore = store;
    }

    /**
     * Creates a snapshot from a versioned LDES. (see: https://semiceu.github.io/LinkedDataEventStreams/#version-materializations)
     * Default:
     * uses "http://example.org/snapshot" as identifier for the snapshot (tree:collection)
     * and uses the current time for ldes:versionMaterializationUntil
     * @param options optional extra parameters for creating the snapshot
     * @param snapshotStore (optional) a snapshot Store (can be used to create an incremental snapshot on top of an existing one)
     * @return {Promise<Store>}
     */
    async create(options: ISnapshotOptions, snapshotStore?: Store): Promise<Store> {
        options.date = options.date ?? new Date();
        options.snapshotIdentifier = options.snapshotIdentifier ?? 'http://example.org/snapshot';
        options.timestampPath = options.timestampPath ?? retrieveTimestampProperty(this.baseStore, options.ldesIdentifier)
        options.versionOfPath = options.versionOfPath ?? retrieveVersionOfProperty(this.baseStore, options.ldesIdentifier)

        const store = createSnapshotMetadata(options)
        const memberStream = storeAsMemberStream(this.baseStore)
        const snapshotTransformer = new SnapshotTransform(options, snapshotStore);
        const transformationOutput = memberStream.pipe(snapshotTransformer)
        const transformedStore = await memberStreamtoStore(transformationOutput, options.snapshotIdentifier)
        store.addQuads(transformedStore.getQuads(null, null, null, null))

        return store
    }

}

