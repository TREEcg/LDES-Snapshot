/***************************************
 * Title: TODO
 * Description: TODO
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 07/03/2022
 *****************************************/
import {Transform, TransformCallback} from 'stream';
import {Member} from '@treecg/types'
import {DataFactory, Literal, Store} from "n3";
import {LDES, RDF, TREE, XSD} from "./Vocabularies";
import {NamedNode} from "@rdfjs/types";
import {dateToLiteral, extractDateFromLiteral} from "./TimestampUtil";
import namedNode = DataFactory.namedNode;
import quad = DataFactory.quad;
import {materialize} from "@treecg/version-materialize-rdf.js";
import {Quad} from "@rdfjs/types";

export interface ISnapshotOptions {
    date?: Date;
    snapshotIdentifier?: string;
    ldesIdentifier: string;
    versionOfPath: string;
    timestampPath: string;
}

export class VersionAwareEventStream extends Transform {
    // materializedMap is a map that has as key the version identifier and as value the materialized quads of the member
    private materializedMap: Map<string, Array<Quad>>;
    // a map that has as key the version identifier and as a value the time of the current saved (in materializedMap)
    // materialized version of that version object
    private versionTimeMap: Map<string, Date>;

    private readonly date: Date;
    private readonly snapshotIdentifier: string;
    private readonly ldesIdentifier: string;
    private readonly versionOfPath: string;
    private readonly timestampPath: string;

    private emitedMetadata: boolean;
    private metadataStore: Store;

    public constructor(options: ISnapshotOptions) {
        super({objectMode: true, highWaterMark: 1000});
        this.materializedMap = new Map<string, Array<Quad>>();
        this.versionTimeMap = new Map<string, Date>();

        this.date = options.date ? options.date : new Date();
        this.snapshotIdentifier = options.snapshotIdentifier ? options.snapshotIdentifier : `${options.ldesIdentifier}Snapshot`;
        this.ldesIdentifier = options.ldesIdentifier;
        this.versionOfPath = options.versionOfPath;
        this.timestampPath = options.timestampPath;

        // create metadata for the snapshot
        this.metadataStore = new Store()
        let snapshotIdentifier: NamedNode = namedNode(this.snapshotIdentifier)
        this.metadataStore.add(quad(snapshotIdentifier, namedNode(RDF.type), namedNode(TREE.Collection)))
        this.metadataStore.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationOf), namedNode(this.ldesIdentifier)))
        this.metadataStore.add(quad(snapshotIdentifier, namedNode(LDES.versionMaterializationUntil), dateToLiteral(this.date)))
        this.emitedMetadata = false;

        // todo: add logger
    }

    public _transform(chunk: any, _enc: any, done: () => void) {
        // called each member
        if (!this.emitedMetadata) {
            this.emit('metadata', this.metadataStore.getQuads(null, null, null, null))
            this.emitedMetadata=true
        }
        // todo: maybe add some error handling if it is not a member?
        this.someNameNeeded(chunk)
        done()
    }


    _flush() {
        // called at the end

        this.materializedMap.forEach((value, key) => {
            this.push({id:key, quads:value})
        })
        this.push(null)
    }

    private someNameNeeded(member: Member) {
        if (this.materializedMap.has(member.id.value)) {
        } else {
            //first time member
            const materialized = materialize(member.quads, {
                versionOfProperty: namedNode(this.versionOfPath),
                timestampProperty: namedNode(this.timestampPath)
            })
            const date = this.extractDate(member)
            const id = this.extractVersionId(member)
            this.materializedMap.set(id, materialized)
            this.versionTimeMap.set(id, date)

        }
    }

    // note: only handles xsd:dateTime
    private extractDate(member: Member): Date {
        const store = new Store(member.quads)
        const dateTimeLiterals = store.getObjects(member.id, namedNode(this.timestampPath), null)
        if (dateTimeLiterals.length !== 1) {
            throw Error(`Found ${dateTimeLiterals.length} timestamp paths for ${member.id.value}, only expected one.`)
        }
        return extractDateFromLiteral(dateTimeLiterals[0] as Literal)
    }

    // note: use the raw member, not the materialized
    private extractVersionId(member: Member) {
        const store = new Store(member.quads)
        const versionIds = store.getObjects(member.id, namedNode(this.versionOfPath),null)
        if (versionIds.length !== 1) {
            throw Error(`Found ${versionIds.length} version paths for ${member.id.value}, only expected one.`)
        }
        return versionIds[0].value
    }
}

