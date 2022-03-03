/***************************************
 * Title: Snapshot
 * Description: Class to create a materialized LDES at a certain timestamp (only works for small LDESes)
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 03/03/2022
 *****************************************/
import {DataFactory, Literal, Store} from "n3";
import {IMaterializeOptions, materialize} from "@treecg/version-materialize-rdf.js";
import namedNode = DataFactory.namedNode;
import {DCT, LDES, RDF, TREE} from "./util/Vocabularies";
import quad = DataFactory.quad;
import {dateToLiteral, extractTimestampFromLiteral, timestampToLiteral} from "./util/TimestampUtil";
import {storeToString} from "./util/Conversion";
import {
    NamedNode
} from "@rdfjs/types";

export class Snapshot {
    private baseStore: Store;
    private materializedStore: Store;
    private versionOfProperty: NamedNode;
    private timestampProperty: NamedNode;

    constructor(store: Store) {
        this.baseStore = store;
        this.versionOfProperty = namedNode(this.retrieveVersionOfProperty())
        this.timestampProperty = namedNode(this.retrieveTimestampProperty())
        this.materializedStore = new Store(materialize(this.baseStore.getQuads(null, null, null, null), {
            "versionOfProperty": this.versionOfProperty,
            "timestampProperty": this.timestampProperty
        }))
    }

    private retrieveVersionOfProperty(): string {
        const versionOfProperties = this.baseStore.getObjects(null, LDES.versionOfPath, null)
        if (versionOfProperties.length !== 1) {
            throw Error(`Found ${versionOfProperties.length} versionOfProperties, only expected one`)
        }
        return versionOfProperties[0].id
    }

    private retrieveTimestampProperty(): string {
        const timestampProperties = this.baseStore.getObjects(null, LDES.timestampPath, null)
        if (timestampProperties.length !== 1) {
            throw Error(`Found ${timestampProperties.length} timestampProperties, only expected one`)
        }
        return timestampProperties[0].id
    }

    materialize(options?: IMaterializeOptions) {
        // todo: retrieve them from store and throw error if they are not present in the ldes
        if (!options) {
            options = {
                "versionOfProperty": namedNode(this.retrieveVersionOfProperty()),
                "timestampProperty": namedNode(this.retrieveTimestampProperty()),
            };
        }
        this.materializedStore = new Store(materialize(this.baseStore.getQuads(null, null, null, null), options))
        this.timestampProperty = options.timestampProperty
        this.versionOfProperty = options.versionOfProperty
    }

    /**
     * Creates a snapshot at current timestamp
     * or at a given timestamp
     */
    create(dateTimeObject: Date): Store {
        const snapshotStore = new Store()
        // add version specific triples: `ldes:versionMaterializationOf` and `ldes:versionMaterializationUntil`
        // Note: materializedStore doesn't handle triples well that are not part of a member
        if (this.baseStore.getSubjects(RDF.type, LDES.EventStream, null).length !== 1) {
            throw Error(`Found ${this.baseStore.getSubjects(RDF.type, LDES.EventStream, null)} LDESs, only expected one`)
        }
        const ldesIdentifier = this.baseStore.getSubjects(RDF.type, LDES.EventStream, null)[0]
        const snapShotIdentifier = namedNode('http://example.org/snapshot') // todo: make configurable
        snapshotStore.add(quad(snapShotIdentifier, namedNode(RDF.type), namedNode(TREE.Collection)))
        snapshotStore.add(quad(snapShotIdentifier, namedNode(LDES.versionMaterializationOf), ldesIdentifier))
        snapshotStore.add(quad(snapShotIdentifier, namedNode(LDES.versionMaterializationUntil), dateToLiteral(dateTimeObject)))

        // versionObjects are the materialized version objects
        // They will become the only members of the new tree:Collection
        const versionObjects = this.materializedStore.getObjects(null, TREE.member, null)

        // per version object, take the most recent one and add that to the new store
        for (const versionObject of versionObjects) {
            // the different version ids of the versionObject
            const versions = this.materializedStore.getObjects(versionObject, DCT.hasVersion, null)
            // most recent till snapshot date
            let mostRecentTimestamp = 0
            let mostRecentId = null

            for (const {id} of versions) {
                // note: only handles xsd:dateTime
                let dateTimeLiterals;
                if (isValidHttpUrl(id)) {
                    dateTimeLiterals = this.materializedStore.getObjects(id, this.timestampProperty, null)
                } else {
                    //blank node
                    dateTimeLiterals = this.materializedStore.getObjects(namedNode('_:'+id), this.timestampProperty, null)
                }

                if (dateTimeLiterals.length !== 1) {
                    throw Error(`Found ${dateTimeLiterals.length} dateTimeLiterals for version ${id}, only expected one.`)
                }
                const versionTime = extractTimestampFromLiteral(dateTimeLiterals[0] as Literal)
                // calculate the most recent timestamp which is not newer than the given timestamp (the dateTimeObject)
                if (versionTime <= dateTimeObject.getTime() && mostRecentTimestamp < versionTime) {
                    mostRecentTimestamp = versionTime
                    mostRecentId = id
                }

            }
            // add shapshot member to collection
            if (mostRecentId) {
                const quads = this.materializedStore.getQuads(versionObject, null, null, namedNode(mostRecentId))
                snapshotStore.addQuads(quads)
                snapshotStore.add(quad(namedNode(versionObject.id), this.timestampProperty, timestampToLiteral(mostRecentTimestamp)))
            }
        }
        return snapshotStore
    }

}
// copied from https://stackoverflow.com/a/43467144
function isValidHttpUrl(string: string) {
    let url;

    try {
        url = new URL(string);
    } catch (_) {
        return false;
    }

    return url.protocol === "http:" || url.protocol === "https:";
}
