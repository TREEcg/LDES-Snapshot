/***************************************
 * Title: Conversion
 * Description: Conversion functions
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 10/12/2021
 *****************************************/
import {DataFactory, Store, Writer} from "n3";
import {ParseOptions} from "rdf-parse/lib/RdfParser";
import {Readable} from "stream";
import {TREE} from "./Vocabularies";
import namedNode = DataFactory.namedNode;
import {Member} from "@treecg/types";

const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require('streamify-string');

export async function turtleStringToStore(text: string, baseIRI?: string): Promise<Store> {
    return await stringToStore(text, {contentType: 'text/turtle', baseIRI});
}

export async function ldjsonToStore(text: string, baseIRI?: string): Promise<Store> {
    return await stringToStore(text, {contentType: 'application/ld+json', baseIRI});
}

/**
 * Converts a store to turtle string
 * @param store
 * @returns {string}
 */
export function storeToString(store: Store): string {
    const writer = new Writer();
    return writer.quadsToString(store.getQuads(null, null, null, null));
}

export async function stringToStore(text: string, options: ParseOptions): Promise<Store> {
    const textStream = streamifyString(text);
    const quadStream = rdfParser.parse(textStream, options);
    return await storeStream(quadStream);
}

/**
 * From an N3 store to create a member stream https://github.com/TREEcg/types/blob/main/lib/Member.ts
 * @param store
 * @returns {Readable}
 */
export function storeAsMemberStream(store: Store): Readable {
    // todo: retrieve member
    const ldesIdentifier = store.getSubjects(TREE.member, null, null)[0].value
    const members = extractMembers(store, ldesIdentifier)
    const myReadable = new Readable({
        objectMode: true,
        read() {
            for (let member of members) {
                this.push(member)
            }
            this.push(null)
        }
    })
    return myReadable
}

/**
 * extract members without containment triple
 * @param store
 * @param ldesIdentifier
 * @returns {Store<Quad, Quad, Quad, Quad>[]}
 */
export function extractMembers(store: Store, ldesIdentifier: string): Member[] {
    const memberSubjects = store.getObjects(ldesIdentifier, TREE.member, null)
    const members : Member[] = memberSubjects.map(memberSubject => {
        return {
            id: memberSubject,
            quads: store.getQuads(memberSubject, null, null, null)

        }
    })

    // extract every member based on the subject
    const mainSubjects = new Set(memberSubjects.map(subj => subj.id));

    for (const member of members) {
        // to avoid issues with data referencing themselves in a circle,
        // duplicates are filtered out as well
        // the initial subject (there should only be one still) is added
        // as an initial to-be-ignored object
        const existingObjects = new Set<string>(member.id.value);
        for (const quad of member.quads) {
            if (existingObjects.has(quad.object.value)) {
                continue;
            }
            existingObjects.add(quad.object.value);
            // all quads with subjects equal to its object representation
            // gets added to this resource entry, so the original subjects'
            // data is completely present inside this single resource
            // this approach already works recursively, as push adds new elements
            // to the end, making them appear as subjects in further
            // iterations
            // quads having another main resource (that is not the current resource)
            // as object are getting filtered out as well, as they cannot be further
            // defined within this single resource
            member.quads.push(
                ...store.getQuads(quad.object, null, null, null).filter((obj) => {
                    return obj.object.id === member.id.value|| !((mainSubjects as Set<string>).has(obj.object.id))
                })
            );
        }
    }
    return members
}

/**
 * From a member stream https://github.com/TREEcg/types/blob/main/lib/Member.ts to a N3 store
 * @param memberStream
 * @param collectionIdentifier
 * @returns {Store}
 */
export async function memberStreamtoStore(memberStream: Readable, collectionIdentifier?: string): Promise<Store> {
    const store = new Store();
    for await (const member of memberStream) {
        store.addQuads(member.quads)
        if (collectionIdentifier) {
            store.addQuad(namedNode(collectionIdentifier), namedNode(TREE.member), member.id)
        }
    }
    return store
}
