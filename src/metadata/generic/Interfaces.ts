/***************************************
 * Title: Interfaces
 * Description: Contains generic interfaces
 * Author: Wout Slabbinck (wout.slabbinck@ugent.be)
 * Created on 30/11/2022
 *****************************************/
import {Store} from "n3";

/**
 * Provides methods to work with an N3 Store.
 */
export interface N3Support {
    getStore: () => Store
}
