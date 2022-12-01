/***************************************
 * Title: AbstractMetadataParser.ts
 * Description: A class that parses metadata for a generic LDES.
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
import {Store} from "n3";
import {LDES, RDF} from "../util/Vocabularies";

/**
 * The {@link AbstractMetadataParser} contains generic static methods to parse parts of an LDES.
 */
export abstract class AbstractMetadataParser {

    /**
     * Extracts exactly one LDES identifier from an N3 Store.
     * @param store the N3 store.
     * @returns {string}
     */
    protected static parseLDESIdentifier(store: Store): string {
        if (store.getSubjects(RDF.type, LDES.EventStream, null).length !== 1) {
            throw Error(`Expected only one Event Stream. ${store.getSubjects(RDF.type, LDES.EventStream, null).length} are present.`)
        }
        return store.getSubjects(RDF.type, LDES.EventStream, null)[0].value
    }

    /**
     * Extracts the timestamp path of an LDES from an N3 Store.
     * @param store the N3 store.
     * @param ldesIdentifier the LDES identifier
     * @returns {string}
     */
    protected static parseTimestampPath(store: Store, ldesIdentifier: string): string {
        if (store.getObjects(ldesIdentifier, LDES.timestampPath, null).length !== 1) {
            throw Error(`Expected one timestampPath. ${store.getObjects(ldesIdentifier, LDES.timestampPath, null).length} are present.`)
        }
        return store.getObjects(ldesIdentifier, LDES.timestampPath, null)[0].value;
    }

    /**
     * Extracts the versionOf path of an LDES from an N3 Store.
     * @param store the N3 store.
     * @param ldesIdentifier the LDES identifier
     * @returns {string}
     */
    protected static parseVersionOfPath(store: Store, ldesIdentifier: string): string {
        if (store.getObjects(ldesIdentifier, LDES.versionOfPath, null).length !== 1) {
            throw Error(`Expected one versionOfPath. ${store.getObjects(ldesIdentifier, LDES.versionOfPath, null).length} are present.`)
        }
        return store.getObjects(ldesIdentifier, LDES.versionOfPath, null)[0].value;
    }
}
