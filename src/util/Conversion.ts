/***************************************
 * Title: Conversion
 * Description: Conversion functions
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 10/12/2021
 *****************************************/
import {Store, Writer} from "n3";
import {ParseOptions} from "rdf-parse/lib/RdfParser";
import {readFileSync} from "fs";
import Path from "path";

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
 * Convert a file as a store (given a path). Default will use text/turtle as content type
 * @param path
 * @param contentType
 * @returns {Promise<Store>}
 */
export async function fileAsStore(path: string, contentType?: string): Promise<Store> {
  contentType = contentType ? contentType : 'text/turtle';
  const text = readFileSync(Path.join(path), "utf8");
  return await stringToStore(text, {contentType});
}
