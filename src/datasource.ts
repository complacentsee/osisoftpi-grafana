import * as lodash from 'lodash';
import { DataSourceInstanceSettings, MetricFindValue} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv, getBackendSrv, TemplateSrv } from '@grafana/runtime';
import { PIWebAPIQuery, PIWebAPIDataSourceJsonData } from './types';
// TODO: Remove depricated calls and replace with rxjs fetch
//import { lastValueFrom } from 'rxjs';

interface PiDataServer {
  name: string | undefined;
  webid: string | undefined;
}

export interface PiwebapiRsp {
  Name?: string;
  InstanceType?: string;
  Items?: PiwebapiRsp[];
  WebId?: string;
  HasChildren?: boolean;
  Path?: string;
}

export interface PiwebapiInternalRsp {
  data: PiwebapiRsp;
  status: number;
  url: string;
}

export class PiWebAPIDatasource extends DataSourceWithBackend<PIWebAPIQuery, PIWebAPIDataSourceJsonData> {
  piserver: PiDataServer;
  afserver: PiDataServer;
  afdatabase: PiDataServer;
  templateSrv: TemplateSrv;
  
  constructor(instanceSettings: DataSourceInstanceSettings<PIWebAPIDataSourceJsonData>) {
    super(instanceSettings);
    this.templateSrv = getTemplateSrv();
    this.piserver = { name: (instanceSettings.jsonData || {}).piserver, webid: undefined };
    this.afserver = { name: (instanceSettings.jsonData || {}).afserver, webid: undefined };
    this.afdatabase = { name: (instanceSettings.jsonData || {}).afdatabase, webid: undefined };
    this.annotations = {};
  }


  /**
   * This method does the discovery of the AF Hierarchy and populates the query user interface segments.
   *
   * @param {any} query - Parses the query configuration and builds a PI Web API query.
   * @returns - Segment information.
   *
   * @memberOf PiWebApiDatasource
   */

  async metricFindQuery(query: any, queryOptions: any): Promise<MetricFindValue[]> {
    let ds = this;
    let querydepth = ['servers', 'databases', 'databaseElements', 'elements'];
    if (typeof query === 'string') {
      query = JSON.parse(query as string);
    }
    if (queryOptions.isPiPoint) {
      query.path = this.templateSrv.replace(query.path, queryOptions);
    } else {
      if (query.path === '') {
        query.type = querydepth[0];
      } else if (query.type !== 'attributes') {
        query.type = querydepth[Math.max(0, Math.min(query.path.split('\\').length, querydepth.length - 1))];
      }
      query.path = this.templateSrv.replace(query.path, queryOptions);
      query.path = query.path.replace(/\{([^\\])*\}/gi, (r: string) => r.substring(1, r.length - 2).split(',')[0]);
    }

    query.filter = query.filter ?? '*';


    if (query.type === 'servers') {
      return ds.afserver?.name
        ? ds.getAssetServer(ds.afserver.name)
            .then((result: PiwebapiRsp) => [result])
            .then(ds.metricQueryTransform)
        : ds.getAssetServers().then(ds.metricQueryTransform);
    } 
    else if (query.type === 'databases') {
      return ds
        .getAssetServer(query.path)
        .then((server) => ds.getDatabases(server.WebId ?? '', {}))
        .then(ds.metricQueryTransform);
    } 
    else if (query.type === 'databaseElements') {
      return ds
        .getDatabase(query.path)
        .then((db) =>
          ds.getDatabaseElements(db.WebId ?? '', {
            selectedFields: 'Items.WebId%3BItems.Name%3BItems.Items%3BItems.Path%3BItems.HasChildren',
          })
        )
        .then(ds.metricQueryTransform);
    } 
    else if (query.type === 'elements') {
      return ds
        .getElement(query.path)
        .then((element) =>
          ds.getElements(element.WebId ?? '', {
            selectedFields: `Items.WebId%3BItems.Name%3BItems.Items%3BItems.Path%3BItems.HasChildren`,
            nameFilter: query.filter,
          })
        )
        .then(ds.metricQueryTransform);
    } 
    else if (query.type === 'attributes') {
      return ds
        .getElement(query.path)
        .then((element) =>
          ds.getAttributes(element.WebId ?? '', {
            searchFullHierarchy: 'true',
            selectedFields: `Items.WebId%3BItems.Name%3BItems.Path`,
            nameFilter: query.filter,
          })
        )
        .then(ds.metricQueryTransform);
    } 
    else if (query.type === 'dataserver') {
      return ds.getDataServers().then(ds.metricQueryTransform);
    } 
    else if (query.type === 'pipoint') {
      return ds.piPointSearch(query.webId, query.pointName).then(ds.metricQueryTransform);
    }
    return Promise.reject('Bad type');
  }

  applyTemplateVariables(query: PIWebAPIQuery): PIWebAPIQuery {
    if (!query || !query.query) {
      return query;
    }

    return {
      ...query,
      query: getTemplateSrv().replace(query.query),
    };
  }

/**
 * Builds the Grafana metric segment for use on the query user interface.
 *
 * @param {any} response - response from PI Web API.
 * @returns {MetricFindValue[]} - Grafana metric segment.
 *
 * @memberOf PiWebApiDatasource
 */
private metricQueryTransform(response: PiwebapiRsp[]): MetricFindValue[] {
  return lodash.map(response, (item) => {
    return {
      text: item.Name,
      expandable: item.HasChildren === undefined || item.HasChildren === true || (item.Path ?? '').split('\\').length <= 3,
      HasChildren: item.HasChildren,
      Items: item.Items ?? [],
      Path: item.Path,
      WebId: item.WebId,
    } as MetricFindValue;
  });
}

/**
 * Abstraction for calling the PI Web API REST endpoint
 *
 * @param {string} path - the path to append to the base server URL.
 * @returns {Promise<PiwebapiInternalRsp>} - The full URL.
 *
 * @memberOf PiWebApiDatasource
 */
private restGet(path: string): Promise<PiwebapiInternalRsp> {
  return getBackendSrv()
    .datasourceRequest({
      url: `/api/datasources/${this.id}/resources/${path}`,
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    })
    .then((response: any) => {
      return response as PiwebapiInternalRsp;
    });
}

  // Get a list of all data (PI) servers
  private getDataServers(): Promise<PiwebapiRsp[]> {
    return this.restGet('/dataservers').then((response) => response.data.Items ?? []);
  }
  // private getDataServer(name: string | undefined): Promise<PiwebapiRsp> {
  //   if (!name) {
  //     return Promise.resolve({});
  //   }
  //   return this.restGet('/dataservers?name=' + name).then((response) => response.data);
  // }

  
  // Get a list of all asset (AF) servers
  private getAssetServers(): Promise<PiwebapiRsp[]> {
    return this.restGet('/assetservers').then((response) => response.data.Items ?? []);
  }

  private getAssetServer(name: string | undefined): Promise<PiwebapiRsp> {
    if (!name) {
      return Promise.resolve({});
    }
    return this.restGet('/assetservers?path=\\\\' + name).then((response) => response.data);
  }
  private getDatabase(path: string | undefined): Promise<PiwebapiRsp> {
    if (!path) {
      return Promise.resolve({});
    }
    return this.restGet('/assetdatabases?path=\\\\' + path).then((response) => response.data);
  }
  getDatabases(serverId: string, options?: any): Promise<PiwebapiRsp[]> {
    if (!serverId) {
      return Promise.resolve([]);
    }
    return this.restGet('/assetservers/' + serverId + '/assetdatabases').then((response) => response.data.Items ?? []);
  }
  getElement(path: string): Promise<PiwebapiRsp> {
    if (!path) {
      return Promise.resolve({});
    }
    console.log("datasource.getElement: ", path)
    return this.restGet('/elements?path=\\\\' + path).then((response) => response.data);
  }
  getEventFrameTemplates(databaseId: string): Promise<PiwebapiRsp[]> {
    if (!databaseId) {
      return Promise.resolve([]);
    }
    return this.restGet(
      '/assetdatabases/' + databaseId + '/elementtemplates?selectedFields=Items.InstanceType%3BItems.Name%3BItems.WebId'
    ).then((response) => {
      return lodash.filter(response.data.Items ?? [], (item) => item.InstanceType === 'EventFrame');
    });
  }
  getElementTemplates(databaseId: string): Promise<PiwebapiRsp[]> {
    if (!databaseId) {
      return Promise.resolve([]);
    }
    return this.restGet(
      '/assetdatabases/' + databaseId + '/elementtemplates?selectedFields=Items.InstanceType%3BItems.Name%3BItems.WebId'
    ).then((response) => {
      return lodash.filter(response.data.Items ?? [], (item) => item.InstanceType === 'Element');
    });
  }

  /**
   * @description
   * Get the child attributes of the current resource.
   * GET attributes/{webId}/attributes
   * @param {string} elementId - The ID of the parent resource. See WebID for more information.
   * @param {Object} options - Query Options
   * @param {string} options.nameFilter - The name query string used for finding attributes. The default is no filter. See Query String for more information.
   * @param {string} options.categoryName - Specify that returned attributes must have this category. The default is no category filter.
   * @param {string} options.templateName - Specify that returned attributes must be members of this template. The default is no template filter.
   * @param {string} options.valueType - Specify that returned attributes' value type must be the given value type. The default is no value type filter.
   * @param {string} options.searchFullHierarchy - Specifies if the search should include attributes nested further than the immediate attributes of the searchRoot. The default is 'false'.
   * @param {string} options.sortField - The field or property of the object used to sort the returned collection. The default is 'Name'.
   * @param {string} options.sortOrder - The order that the returned collection is sorted. The default is 'Ascending'.
   * @param {string} options.startIndex - The starting index (zero based) of the items to be returned. The default is 0.
   * @param {string} options.showExcluded - Specified if the search should include attributes with the Excluded property set. The default is 'false'.
   * @param {string} options.showHidden - Specified if the search should include attributes with the Hidden property set. The default is 'false'.
   * @param {string} options.maxCount - The maximum number of objects to be returned per call (page size). The default is 1000.
   * @param {string} options.selectedFields - List of fields to be returned in the response, separated by semicolons (;). If this parameter is not specified, all available fields will be returned. See Selected Fields for more information.
   */
  private getAttributes(elementId: string, options: any): Promise<PiwebapiRsp[]> {
    var querystring =
      '?' +
      lodash.map(options, (value, key) => {
        return key + '=' + value;
      }).join('&');

    if (querystring === '?') {
      querystring = '';
    }

    return this.restGet('/elements/' + elementId + '/attributes' + querystring).then(
      (response) => response.data.Items ?? []
    );
  }

  /**
   * @description
   * Retrieve elements based on the specified conditions. By default, this method selects immediate children of the current resource.
   * Users can search for the elements based on specific search parameters. If no parameters are specified in the search, the default values for each parameter will be used and will return the elements that match the default search.
   * GET assetdatabases/{webId}/elements
   * @param {string} databaseId - The ID of the parent resource. See WebID for more information.
   * @param {Object} options - Query Options
   * @param {string} options.webId - The ID of the resource to use as the root of the search. See WebID for more information.
   * @param {string} options.nameFilter - The name query string used for finding objects. The default is no filter. See Query String for more information.
   * @param {string} options.categoryName - Specify that returned elements must have this category. The default is no category filter.
   * @param {string} options.templateName - Specify that returned elements must have this template or a template derived from this template. The default is no template filter.
   * @param {string} options.elementType - Specify that returned elements must have this type. The default type is 'Any'. See Element Type for more information.
   * @param {string} options.searchFullHierarchy - Specifies if the search should include objects nested further than the immediate children of the searchRoot. The default is 'false'.
   * @param {string} options.sortField - The field or property of the object used to sort the returned collection. The default is 'Name'.
   * @param {string} options.sortOrder - The order that the returned collection is sorted. The default is 'Ascending'.
   * @param {number} options.startIndex - The starting index (zero based) of the items to be returned. The default is 0.
   * @param {number} options.maxCount - The maximum number of objects to be returned per call (page size). The default is 1000.
   * @param {string} options.selectedFields -  List of fields to be returned in the response, separated by semicolons (;). If this parameter is not specified, all available fields will be returned. See Selected Fields for more information.
   */
  private getDatabaseElements(databaseId: string, options: any): Promise<PiwebapiRsp[]> {
    var querystring =
      '?' +
      lodash.map(options, (value, key) => {
        return key + '=' + value;
      }).join('&');

    if (querystring === '?') {
      querystring = '';
    }

    return this.restGet('/assetdatabases/' + databaseId + '/elements' + querystring).then(
      (response) => response.data.Items ?? []
    );
  }

  /**
   * @description
   * Retrieve elements based on the specified conditions. By default, this method selects immediate children of the current resource.
   * Users can search for the elements based on specific search parameters. If no parameters are specified in the search, the default values for each parameter will be used and will return the elements that match the default search.
   * GET elements/{webId}/elements
   * @param {string} databaseId - The ID of the resource to use as the root of the search. See WebID for more information.
   * @param {Object} options - Query Options
   * @param {string} options.webId - The ID of the resource to use as the root of the search. See WebID for more information.
   * @param {string} options.nameFilter - The name query string used for finding objects. The default is no filter. See Query String for more information.
   * @param {string} options.categoryName - Specify that returned elements must have this category. The default is no category filter.
   * @param {string} options.templateName - Specify that returned elements must have this template or a template derived from this template. The default is no template filter.
   * @param {string} options.elementType - Specify that returned elements must have this type. The default type is 'Any'. See Element Type for more information.
   * @param {string} options.searchFullHierarchy - Specifies if the search should include objects nested further than the immediate children of the searchRoot. The default is 'false'.
   * @param {string} options.sortField - The field or property of the object used to sort the returned collection. The default is 'Name'.
   * @param {string} options.sortOrder - The order that the returned collection is sorted. The default is 'Ascending'.
   * @param {number} options.startIndex - The starting index (zero based) of the items to be returned. The default is 0.
   * @param {number} options.maxCount - The maximum number of objects to be returned per call (page size). The default is 1000.
   * @param {string} options.selectedFields -  List of fields to be returned in the response, separated by semicolons (;). If this parameter is not specified, all available fields will be returned. See Selected Fields for more information.
   */
  private getElements(elementId: string, options: any): Promise<PiwebapiRsp[]> {
    var querystring =
      '?' +
      lodash.map(options, (value, key) => {
        return key + '=' + value;
      }).join('&');

    if (querystring === '?') {
      querystring = '';
    }

    return this.restGet('/elements/' + elementId + '/elements' + querystring).then(
      (response) => response.data.Items ?? []
    );
  }

  /**
   * Retrieve a list of points on a specified Data Server.
   *
   * @param {string} serverId - The ID of the server. See WebID for more information.
   * @param {string} nameFilter - A query string for filtering by point name. The default is no filter. *, ?, [ab], [!ab]
   */
  private piPointSearch(serverId: string, nameFilter: string): Promise<PiwebapiRsp[]> {
    let filter1 = this.templateSrv.replace(nameFilter);
    let filter2 = `${filter1}`;
    let doFilter = false;
    if (filter1 !== nameFilter) {
      const regex = /\{(\w|,)+\}/gs;
      let m;
      while ((m = regex.exec(filter1)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
          regex.lastIndex++;
        }

        // The result can be accessed through the `m`-variable.
        m.forEach((match, groupIndex) => {
          if (groupIndex === 0) {
            filter1 = filter1.replace(match, match.replace('{', '(').replace('}', ')').replace(',', '|'));
            filter2 = filter2.replace(match, '*');
            doFilter = true;
          }
        });
      }
    }
    return this.restGet('/dataservers/' + serverId + '/points?maxCount=20&nameFilter=' + filter2).then((results) => {
      if (!!results && !!results.data?.Items) {
        return doFilter ? results.data.Items.filter((item) => item.Name?.match(filter1)) : results.data.Items;
      }
      return [];
    });
  }

  /**
   * Get the PI Web API webid or PI Point.
   *
   * @param {any} target - AF Path or Point name.
   * @returns - webid.
   *
   * @memberOf PiWebApiDatasource
   */
  getWebId(target: any) {
    var ds = this;
    var isAf = target.target.indexOf('\\') >= 0;
    var isAttribute = target.target.indexOf('|') >= 0;
    if (!isAf && target.target.indexOf('.') === -1) {
      return Promise.resolve([{ WebId: target.target, Name: target.display || target.target }]);
    }

    if (!isAf) {
      // pi point lookup
      return ds.piPointSearch(this.piserver.webid!, target.target).then((results) => {
        if (results === undefined || results.length === 0) {
          return [{ WebId: target.target, Name: target.display || target.target }];
        }
        return results;
      });
    } else if (isAf && isAttribute) {
      // af attribute lookup
      return ds.restGet('/attributes?path=\\\\' + target.target).then((results) => {
        if (results.data === undefined || results.status !== 200) {
          return [{ WebId: target.target, Name: target.display || target.target }];
        }
        // rewrite name if specified
        results.data.Name = target.display || results.data.Name;
        return [results.data];
      });
    } else {
      // af element lookup
      return ds.restGet('/elements?path=\\\\' + target.target).then((results) => {
        if (results.data === undefined || results.status !== 200) {
          return [{ WebId: target.target, Name: target.display || target.target }];
        }
        // rewrite name if specified
        results.data.Name = target.display || results.data.Name;
        return [results.data];
      });
    }
  }


}
