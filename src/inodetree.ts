"use strict";

import { join } from 'path';
import { readJson, writeJson } from 'fs-extra';
import { GFolder } from './folder.es6.js';
import { GFile } from './file.es6.js';

import { config as _config, dataLocation as _dataLocation, uploadLocation as _uploadLocation, downloadLocation as _downloadLocation, logger as _logger, oauth2Client as _oauth2Client, refreshAccessToken as _refreshAccessToken } from './common.es6.js';
const config = _config
const dataLocation = _dataLocation;
const uploadLocation = _uploadLocation;
const downloadLocation = _downloadLocation;
const logger = _logger;
const oauth2Client = _oauth2Client;
const refreshAccessToken = _refreshAccessToken

class InodeTree {
	currentLargestInode: any;
	map: Map<any, any>;
	idToInode: Map<any, any>;
	saving: boolean;
	
	constructor(){
		this.map = new Map();
		this.idToInode = new Map();
		this.currentLargestInode = 0;
		this.saving = false;
	}

	insert(value){
		if( this.map.has(++this.currentLargestInode) ){
			return this.insert(value);
		}else{
			value.inode = this.currentLargestInode;
			this.map.set(this.currentLargestInode, value);

			if(this.currentLargestInode > 1){
				const parent = this.getFromId(value.parentid);

				//if the parent doesn't exist, put it in the root
				if(parent){
					parent.children.push(this.currentLargestInode);
				}else{
					const root = this.getFromInode(1);
					root.children.push(this.currentLargestInode);
					logger.info(`no parent found for item called "${value.name}". Placing it under the root folder`)  
				}
			}

			this.idToInode.set(value.id, value);
			this.saveFolderTree()
		
			return this.currentLargestInode;
		}
	}

	delete(key){
		const value = this.map.get(key);
		if(value){
			this.idToInode.delete(value.id);
		}
		return this.map.delete(key);
	}

	has(key){
		return this.map.has(key);
	}

	getFromInode(key){
		return this.map.get(key);
	}

	getFromId(id){
		return this.idToInode.get(id);
	}

	mapInodeToId(inode,id){
		return this.idToInode.set(inode,id);
	}

	loadFolderTree( callback ){
		const jsonFile =  join(dataLocation, 'inodeTree.json');
		const self = this;

		readJson( jsonFile, (err, data) => {
				if (err) {
					logger.debug(err);
					callback(err);
					return;
				}

				// try{
				for (let key of Object.keys(data)) {
					const o = data[key];
					if (key == "1") {
						const folder = new GFolder(o.id, o.parentid, o.name, o.ctime, o.mitime, o.permission, o.children);
						folder.inode = 1;
						self.map.set(1, folder);
						self.idToInode.set(o.id, folder);
						continue;
					}

					// make sure parent directory exists
					if (!self.idToInode.has(o.parentid)) {
						logger.debug("parent directory did not exist");
						logger.debug(o);
						continue;
					}


					if ('size' in o) {
						const file = new GFile(o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.permission);
						file.inode = o.inode;
						self.map.set(o.inode, file);
						self.idToInode.set(o.id, file);
					} else {
						const folder = new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.permission, o.children);
						folder.inode = o.inode;
						self.map.set(o.inode, folder);
						self.idToInode.set(o.id, folder);
					}
					if (o.inode > self.currentLargestInode) {
						self.currentLargestInode = o.inode;
					}

				}
				callback();

				// }catch(error){
				//     // if there was an error with reading the file, just download the whole structure again
				//     logger.debug(error);
				//     callback(error);
				// }
			});

	}

	saveFolderTree(){
		const self = this;
		if( this.saving){
			return;
		}
		self.saving = true;
	    logger.debug( "saving folder tree");
	    const toSave = {};
	    for( let key of this.map.keys()){
	        const value = this.map.get(key);
	        const saved = {
	            downloadUrl: value.downloadUrl,
	            id: value.id,
	            parentid: value.parentid,
	            name: value.name,
	            ctime: value.ctime,
	            mtime: value.mtime,
	            inode: value.inode,
	            permission: value.permission,
	            mode: value.mode
	        };

	        if(value instanceof GFile){
	            saved.size = value.size;
	        }else{
	            saved.children = value.children;
	        }


	        toSave[key] = saved;
	    }

	    writeJson(join(dataLocation,'inodeTree.json'), toSave,  function saveFolderTreeCallback(err){
	    	if(err){
	    		logger.error("There was an error while saving inodeTree");
	    	}
	    	self.saving = false;
	    });
	
	}

	setNewParents(f, parentid){
		//get parents
		const newParent = this.idToInode.get(parentid);
		const oldParent = this.idToInode.get(f.parentid);

		//change the objects parentid
		f.parentId = newParent.id

		//change the children's inode list
		let idx = oldParent.children.indexOf(f.inode);
		if (idx >= 0) {
		    oldParent.children.splice(idx, 1);
		}
		this.map.delete(f.inode);

		idx = newParent.children.indexOf(f.inode);
		if(idx < 0){
			newParent.children.push(f.inode);
		}


	}


}

export default new InodeTree();