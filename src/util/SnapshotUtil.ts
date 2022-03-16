import {DataFactory, Store} from "n3";
import {NamedNode} from "@rdfjs/types";
import {LDES, RDF, TREE} from "./Vocabularies";
import {dateToLiteral} from "./TimestampUtil";
import {ISnapshotOptions} from "../SnapshotTransform";
import namedNode = DataFactory.namedNode;
import quad = DataFactory.quad;

/***************************************
 * Title: snapshotUtil
 * Description: utility functions used in and for the SnapshotTransform
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 08/03/2022
 *****************************************/

/**
 * creates a store that corresponds to the metadata of a snapshot
 * @param options snapshot configuration
 * @return {Store}
 */
export function createSnapshotMetadata(options: ISnapshotOptions): Store {
    options.date = options.date ? options.date : new Date();
    options.snapshotIdentifier = options.snapshotIdentifier ? options.snapshotIdentifier : `${options.ldesIdentifier}Snapshot`;

    const store = new Store()
    let snapshotIdentifier: NamedNode = namedNode(options.snapshotIdentifier)
    store.add(quad(snapshotIdentifier, namedNode(RDF.type), namedNode(TREE.Collection)))
    store.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationOf), namedNode(options.ldesIdentifier)))
    store.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationUntil), dateToLiteral(options.date)))
    return store
}

/**
 * Retrieves the versionOfPath of a version LDES
 * @param store
 * @param ldesIdentifier
 * @returns {string}
 */
export function retrieveVersionOfProperty(store: Store, ldesIdentifier: string): string {
    const versionOfProperties = store.getObjects(namedNode(ldesIdentifier), LDES.versionOfPath, null)
    if (versionOfProperties.length !== 1) {
        // https://semiceu.github.io/LinkedDataEventStreams/#version-materializations
        // A version materialization can be defined only if the original LDES defines both ldes:versionOfPath and ldes:timestampPath.
        throw Error(`Found ${versionOfProperties.length} versionOfProperties for ${ldesIdentifier}, only expected one`)
    }
    return versionOfProperties[0].id
}

/**
 * Retrieves the timestampPath of a version LDES
 * @param store
 * @param ldesIdentifier
 * @returns {string}
 */
export function retrieveTimestampProperty(store: Store, ldesIdentifier: string): string {
    const timestampProperties = store.getObjects(namedNode(ldesIdentifier), LDES.timestampPath, null)
    if (timestampProperties.length !== 1) {
        // https://semiceu.github.io/LinkedDataEventStreams/#version-materializations
        // A version materialization can be defined only if the original LDES defines both ldes:versionOfPath and ldes:timestampPath.
        throw Error(`Found ${timestampProperties.length} timestampProperties for ${ldesIdentifier}, only expected one`)
    }
    return timestampProperties[0].id
}

/**
 * Creates ISnapshotOptions from a N3 Store which contains a versioned LDES
 * @param store
 * @param ldesIdentifier
 * @returns {{versionOfPath: string, ldesIdentifier: string, timestampPath: string}}
 */
export function extractSnapshotOptions(store: Store, ldesIdentifier: string): ISnapshotOptions {
    return {
        ldesIdentifier: ldesIdentifier,
        timestampPath: retrieveTimestampProperty(store, ldesIdentifier),
        versionOfPath: retrieveVersionOfProperty(store, ldesIdentifier),
    }
}

export function isMember(data: any): boolean {
    if (typeof data !== 'object' &&
        !Array.isArray(data) &&
        data !== null) {
        return false
    }
    if (!(data.id && typeof data.id.value === 'string')) {
        return false
    }

    if (data.quads && Array.isArray(data.quads)) {
        if (data.quads.length > 0 && data.quads[0].termType === 'Quad') {
            return true
        } else return false
    } else return false
}
