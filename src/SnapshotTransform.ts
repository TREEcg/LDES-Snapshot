/***************************************
 * Title: SnapshotTransform
 * Description: Transforms a Member stream to a stream of materialized Members at a given snapshot time.
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 07/03/2022
 *****************************************/
import {Transform} from 'stream';
import {Member} from '@treecg/types'
import {DataFactory, Literal, Store} from "n3";
import {Quad} from "@rdfjs/types";
import {extractDateFromLiteral} from "./util/TimestampUtil";
import {materialize} from "@treecg/version-materialize-rdf.js";
import {createSnapshotMetadata, isMember} from "./util/SnapshotUtil";
import {Logger} from "./logging/Logger";
import namedNode = DataFactory.namedNode;
import quad = DataFactory.quad;

export interface ISnapshotOptions {
    date?: Date;
    snapshotIdentifier?: string;
    ldesIdentifier: string;
    versionOfPath?: string;
    timestampPath?: string;
    materialized?: boolean;
}

export class SnapshotTransform extends Transform {
    private readonly logger = new Logger(this);

    // transformedMap is a map that has as key the version identifier and as value the transformed quads of the member
    private transformedMap: Map<string, Array<Quad>>;
    // a map that has as key the version identifier and as a value the time of the current saved (in transformedMap)
    // transformed version of that version object
    private versionTimeMap: Map<string, Date>;

    private readonly date: Date;
    private readonly snapshotIdentifier: string;
    private readonly ldesIdentifier: string;
    private readonly versionOfPath: string;
    private readonly timestampPath: string;
    private readonly materialized: boolean;

    private emitedMetadata: boolean;
    private metadataStore: Store;

    public constructor(options: ISnapshotOptions) {
        super({objectMode: true, highWaterMark: 1000});
        if (!options.versionOfPath) throw new Error("No versionOfPath was given in options")
        if (!options.timestampPath) throw new Error("No timestampPath was given in options")
        this.transformedMap = new Map<string, Array<Quad>>();
        this.versionTimeMap = new Map<string, Date>();

        this.date = options.date ? options.date : new Date();
        this.snapshotIdentifier = options.snapshotIdentifier ? options.snapshotIdentifier : `${options.ldesIdentifier}Snapshot`;
        this.ldesIdentifier = options.ldesIdentifier;
        this.versionOfPath = options.versionOfPath;
        this.timestampPath = options.timestampPath;
        this.materialized = options.materialized ? options.materialized : false;

        // create metadata for the snapshot
        this.metadataStore = createSnapshotMetadata({
            date: this.date,
            snapshotIdentifier: this.snapshotIdentifier,
            ldesIdentifier: this.ldesIdentifier,
            versionOfPath: this.versionOfPath,
            timestampPath: this.timestampPath,
            materialized: this.materialized
        })
        this.emitedMetadata = false;

    }

    public _transform(chunk: any, _enc: any, done: () => void) {
        // called each member
        if (!this.emitedMetadata) {
            this.emit('metadata', this.metadataStore.getQuads(null, null, null, null))
            this.emitedMetadata = true
        }

        try {
            this.processMember(chunk)
        } catch (e) {
            if (isMember(chunk)) {
                this.logger.info(`Following member could not be transformed: ${chunk.id.value}`)
            } else {
                this.logger.info('item in stream was not a member: ' + chunk)
            }
            console.log(e)
        }
        done()
    }


    _flush() {
        // called at the end

        this.transformedMap.forEach((value, key) => {
            //Note: can be wrong if not all subjects of the quads are the same id
            // -> could be solved in a more thorough isMember test before processing the members
            this.push({id: value[0].subject, quads: value})

        })
        this.push(null)
    }

    private processMember(member: Member) {
        const versionObjectID = this.extractVersionId(member)

        if (this.transformedMap.has(versionObjectID)) {
            const versionPresentTime = this.versionTimeMap.get(versionObjectID)!
            const currentTime = this.extractDate(member)
            // dateTime must be more recent than the one already present and not more recent than the snapshotDate
            if (currentTime.getTime() <= this.date.getTime() && versionPresentTime.getTime() < currentTime.getTime()) {
                this.transformedMap.set(versionObjectID, this.transform(member))
                this.versionTimeMap.set(versionObjectID, currentTime)
            }
        } else {
            //first time member
            const transformed = this.transform(member)
            const date = this.extractDate(member)

            if (date.getTime() <= this.date.getTime()) {
                this.transformedMap.set(versionObjectID, transformed)
                this.versionTimeMap.set(versionObjectID, date)
            }
        }
    }

    private transform(member: Member): Quad[] {
        const transformedTriples: Quad[] = []

        if (this.materialized) {
            const materializedQuads = materialize(member.quads, {
                versionOfProperty: namedNode(this.versionOfPath),
                timestampProperty: namedNode(this.timestampPath)
            });
            // code below here is to transform quads to triples
            for (const q of materializedQuads) {
                if (q.predicate.value === this.timestampPath) {
                    // have version object id as indication for the update
                    transformedTriples.push(quad(namedNode(this.extractVersionId(member)), q.predicate, q.object))
                } else {
                    // note: ugly fix to undefined problem, copying all other triples
                    if (q.subject) {
                        transformedTriples.push(quad(q.subject, q.predicate, q.object));
                    } else {
                        transformedTriples.push(quad(namedNode(q.graph.value), q.predicate, q.object));
                    }
                }
            }
        } else {
            transformedTriples.push(...member.quads)
        }
        return transformedTriples
    }

// note: only handles xsd:dateTime
    private extractDate(member: Member): Date {
        const store = new Store(member.quads)
        const dateTimeLiterals = store.getObjects(member.id, namedNode(this.timestampPath), null)
        if (dateTimeLiterals.length !== 1) {
            throw Error(`Found ${dateTimeLiterals.length} dateTime literals following the timestamp path of ${member.id.value}; expected one such literal.`)
        }
        return extractDateFromLiteral(dateTimeLiterals[0] as Literal)
    }

    // note: use the raw member, not the materialized
    private extractVersionId(member: Member) {
        const store = new Store(member.quads)
        const versionIds = store.getObjects(member.id, namedNode(this.versionOfPath), null)
        if (versionIds.length !== 1) {
            throw Error(`Found ${versionIds.length} identifiers following the version paths of ${member.id.value}; expected one such identifier.`)
        }
        return versionIds[0].value
    }
}

