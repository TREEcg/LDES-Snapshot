/***************************************
 * Title: Conversion
 * Description: this conversion deals with IO on file systems
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 16/03/2022
 *****************************************/

import {Store} from "n3";
import {readFileSync} from "fs";
import Path from "path";
import {stringToStore} from "../../src/util/Conversion";

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
