import {DataFactory, Literal, Store} from "n3";
import {NamedNode} from "@rdfjs/types";
import {DCT, LDES, RDF, TREE} from "./Vocabularies";
import {dateToLiteral, extractDateFromLiteral} from "./TimestampUtil";
import {ISnapshotOptions} from "../SnapshotTransform";
import namedNode = DataFactory.namedNode;
import quad = DataFactory.quad;
import {Member} from "@treecg/types";
import {SnapshotMetadataInitializer} from "../metadata/SnapshotMetadataInitializer";

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

    if (options.materialized) {
        store.add(quad(snapshotIdentifier, namedNode(RDF.type), namedNode(TREE.Collection)))
        store.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationOf), namedNode(options.ldesIdentifier)))
        store.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationUntil), dateToLiteral(options.date)))
    } else {
        if (!options.versionOfPath) throw new Error("No versionOfPath was given in options")
        if (!options.timestampPath) throw new Error("No timestampPath was given in options")
        store.addQuads(SnapshotMetadataInitializer.generateSnapshotMetadata(options).getStore().getQuads(null, null, null,null))
    }
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
 * Retrieves the timestampPath of a versioned LDES
 * @param store an N3 store
 * @param ldesIdentifier The identifier of the LDES
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
 * @param store an N3 store
 * @param ldesIdentifier The identifier of the LDES
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

/**
 * Extracts the materialized id from a non-materialized member
 * @param member
 * @param versionOfPath
 * @returns {string}
 */
export function extractMaterializedId(member: Member, versionOfPath: string): string {
    const store = new Store(member.quads)
    const versionIds = store.getObjects(member.id, namedNode(versionOfPath), null)
    if (versionIds.length !== 1) {
        throw Error(`Found ${versionIds.length} identifiers following the version paths of ${member.id.value}; expected one such identifier.`)
    }
    return versionIds[0].value
}

/**
 * Extracts the materialized id from a materialized member
 * @param member
 * @returns {string}
 */
export function extractMaterializedIdMaterialized(member: Member): string {
    const store = new Store(member.quads)
    const versionIds = store.getSubjects(namedNode(DCT.hasVersion), member.id, null)
    if (versionIds.length !== 1) {
        throw Error(`Found ${versionIds.length} identifiers following the version paths of ${member.id.value}; expected one such identifier.`)
    }
    return versionIds[0].value
}

/**
 * Extracts the date from a member. Note: the date must be of type xsd:dateTime
 * @param store N3 Store only containing the member
 * @param timestampPath the `ldes:timestampPath` of the versioned LDES
 */
export function extractDate(store: Store, timestampPath: string): Date {
    const dateTimeLiterals = store.getObjects(null, namedNode(timestampPath), null)
    if (dateTimeLiterals.length !== 1) {
        throw Error(`Found ${dateTimeLiterals.length} dateTime literals.`)
    }
    return extractDateFromLiteral(dateTimeLiterals[0] as Literal)
}

/**
 * Extracts the object Identifier from a member
 * @param store N3 Store only containing the member
 * @param versionOfPath the `ldes:versionOfPath` of the versioned LDES
 */
export function extractObjectIdentifier(store: Store, versionOfPath: string): string {
    const objectIdentifiers = store.getObjects(null, namedNode(versionOfPath), null)
    if (objectIdentifiers.length !== 1) {
        throw Error(`Found ${objectIdentifiers.length} versionOfPaths.`)
    }
    return objectIdentifiers[0].value
}

