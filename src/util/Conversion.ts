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
    const members = store.getObjects(null, TREE.member, null)

    const myReadable = new Readable({
        objectMode: true,
        read() {
            for (let member of members) {
                this.push({
                    id: member,
                    quads: store.getQuads(member, null, null, null)
                })
            }
            this.push(null)
        }
    })
    return myReadable
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
