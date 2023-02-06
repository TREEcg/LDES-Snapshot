/***************************************
 * Title: SnapshotMetadataInitializer.ts
 * Description: A class that generates metadata for a {@link ISnapshot}.
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
import {ISnapshotOptions} from "../SnapshotTransform";
import {ISnapshot, SnapshotMetadata} from "./SnapshotMetadata";
import {DCT} from "../util/Vocabularies";

export class SnapshotMetadataInitializer {
    /**
     * Generate {@link ISnapshot} based on {@link ISnapshotOptions}.
     * @param options
     * @returns {SnapshotMetadata}
     */
    public static generateSnapshotMetadata(options: ISnapshotOptions): ISnapshot {
        let date = options.date ?? new Date()
        return new SnapshotMetadata({
            eventStreamIdentifier: options.snapshotIdentifier ?? "http://example.org/" + date.valueOf(),
            materialized: false,
            members: [],
            snapshotOf: options.ldesIdentifier,
            snapshotUntil: date,
            timestampPath: options.timestampPath ?? DCT.created,
            versionOfPath: options.versionOfPath ?? DCT.isVersionOf
        })
    }
}
