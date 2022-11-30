/***************************************
 * Title: AbstractMetadataParser.ts
 * Description: TODO
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
import {Store} from "n3";
import {LDES, RDF} from "../util/Vocabularies";

export abstract class AbstractMetadataParser {
    protected static parseLDESIdentifier(store: Store): string {
        if (store.getSubjects(RDF.type, LDES.EventStream, null).length !== 1) {
            throw Error(`Expected only one Event Stream. ${store.getSubjects(RDF.type, LDES.EventStream, null).length} are present.`)
        }
        return store.getSubjects(RDF.type, LDES.EventStream, null)[0].value
    }

    protected static parseTimestampPath(store: Store, identifier: string): string {
        if (store.getObjects(identifier, LDES.timestampPath, null).length !== 1) {
            throw Error(`Expected one timestampPath. ${store.getObjects(identifier, LDES.timestampPath, null).length} are present.`)
        }
        return store.getObjects(identifier, LDES.timestampPath, null)[0].value;
    }

    protected static parseVersionOfPath(store: Store, identifier: string): string {
        if (store.getObjects(identifier, LDES.versionOfPath, null).length !== 1) {
            throw Error(`Expected one versionOfPath. ${store.getObjects(identifier, LDES.versionOfPath, null).length} are present.`)
        }
        return store.getObjects(identifier, LDES.versionOfPath, null)[0].value;
    }
}
