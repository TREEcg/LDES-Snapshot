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
import {
    NamedNode
} from "@rdfjs/types";

export class Snapshot {
    private baseStore: Store;
    private materializedStore: Store;
    private versionOfProperty: NamedNode;
    private timestampProperty: NamedNode;
    private readonly ldesIRI: string;

    constructor(store: Store) {
        this.baseStore = store;
        this.ldesIRI = this.retrieveLdesIRI()
        this.versionOfProperty = namedNode(this.retrieveVersionOfProperty())
        this.timestampProperty = namedNode(this.retrieveTimestampProperty())
        this.materializedStore = this.materialize()
    }

    private retrieveLdesIRI(): string {
        const possibleIdentifiers = this.baseStore.getSubjects(RDF.type, LDES.EventStream, null)
        if (possibleIdentifiers.length !== 1) {
            throw Error(`Found ${possibleIdentifiers.length} LDESs, only expected one`)
        }
        return possibleIdentifiers[0].id
    }

    private retrieveVersionOfProperty(): string {
        const versionOfProperties = this.baseStore.getObjects(namedNode(this.ldesIRI), LDES.versionOfPath, null)
        if (versionOfProperties.length !== 1) {
            // https://semiceu.github.io/LinkedDataEventStreams/#version-materializations
            // A version materialization can be defined only if the original LDES defines both ldes:versionOfPath and ldes:timestampPath.
            throw Error(`Found ${versionOfProperties.length} versionOfProperties, only expected one`)
        }
        return versionOfProperties[0].id
    }

    private retrieveTimestampProperty(): string {
        const timestampProperties = this.baseStore.getObjects(namedNode(this.ldesIRI), LDES.timestampPath, null)
        if (timestampProperties.length !== 1) {
            // https://semiceu.github.io/LinkedDataEventStreams/#version-materializations
            // A version materialization can be defined only if the original LDES defines both ldes:versionOfPath and ldes:timestampPath.
            throw Error(`Found ${timestampProperties.length} timestampProperties, only expected one`)
        }
        return timestampProperties[0].id
    }

    /**
     * Materialize the LDES (using @treecg/version-materialize-rdf.js), save this materialized LDES
     * and return it
     * @param options
     * @returns {Store}
     */
    materialize(options?: IMaterializeOptions): Store {
        if (!options) {
            options = {
                "versionOfProperty": this.versionOfProperty,
                "timestampProperty": this.timestampProperty,
            };
        } else {
            this.timestampProperty = options.timestampProperty
            this.versionOfProperty = options.versionOfProperty
        }
        this.materializedStore = new Store(materialize(this.baseStore.getQuads(null, null, null, null), options))
        return this.materializedStore
    }

    /**
     * Creates a snapshot at current timestamp
     * or at a given timestamp
     */
    create(dateTimeObject: Date): Store {
        const snapshotStore = new Store()
        // add version specific triples: `ldes:versionMaterializationOf` and `ldes:versionMaterializationUntil`
        // Note: materializedStore doesn't handle triples well that are not part of a member

        const snapshotIdentifier = namedNode('http://example.org/snapshot') // todo: make configurable
        snapshotStore.add(quad(snapshotIdentifier, namedNode(RDF.type), namedNode(TREE.Collection)))
        snapshotStore.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationOf), namedNode(this.ldesIRI)))
        snapshotStore.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationUntil), dateToLiteral(dateTimeObject)))

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
                    dateTimeLiterals = this.materializedStore.getObjects(namedNode('_:' + id), this.timestampProperty, null)
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
                snapshotStore.addQuads(quads.map(q => quad(q.subject, q.predicate, q.object)))
                snapshotStore.add(quad(namedNode(versionObject.id), this.timestampProperty, timestampToLiteral(mostRecentTimestamp)))
                snapshotStore.add(quad(snapshotIdentifier, namedNode(TREE.member), namedNode(versionObject.id)))
            }
        }
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
