import { successResponse, wrap, IResponse } from "../../../utils/shared";
import { AdaptorRecord, AdaptorRecordType } from "../../db-utils/adaptor-record"
import allSettled from "promise.allsettled";

import { generateAggregatedVolumesChartData, generateByDexVolumesChartData, getSumAllDexsToday, getStatsByProtocolVersion, IChartData, IChartDataByDex, sumAllVolumes } from "../../utils/volumeCalcs";
import { formatChain } from "../../utils/getAllChainsFromAdaptors";
import config from "../../data/volumes/config";
import { sendDiscordAlert } from "../../utils/notify";
import { AdapterType } from "@defillama/adaptors/adapters/types";
import { IRecordAdaptorRecordData } from "../../db-utils/adaptor-record";
import { IJSON, ProtocolAdaptor } from "../../data/types";
import loadAdaptorsData from "../../data"
import generateProtocolAdaptorSummary, { ProtocolAdaptorSummary } from "../helpers/generateProtocolAdaptorSummary";

export interface IGeneralStats {
    total24h: number | null;
    change_1d: number | null;
    change_7d: number | null;
    change_1m: number | null;
    breakdown24h: IRecordAdaptorRecordData | null
}

export interface ProtocolAdaptorSummaryCombined extends Omit<
    ProtocolAdaptorSummary,
    'change_1d' |
    'change_7d' |
    'change_1m' |
    'total24h' |
    'breakdown24h' |
    'protocolsStats' |
    'records' |
    'recordsMap'
> {
    change_1d: IJSON<ProtocolAdaptorSummary['change_1d']>
    change_7d: IJSON<ProtocolAdaptorSummary['change_7d']>
    change_1m: IJSON<ProtocolAdaptorSummary['change_1m']>
    total24h: IJSON<ProtocolAdaptorSummary['total24h']>
    breakdown24h: IJSON<ProtocolAdaptorSummary['breakdown24h']>
    protocolsStats: IJSON<ProtocolAdaptorSummary['protocolsStats']>
    records: IJSON<ProtocolAdaptorSummary['records']>
    recordsMap: IJSON<ProtocolAdaptorSummary['recordsMap']>
}

type KeysToRemove = 'records' | 'module' | 'config' | 'recordsMap'
type ProtocolsResponse = Omit<ProtocolAdaptorSummaryCombined, KeysToRemove>
export type IGetOverviewResponseBody = IGeneralStats & {
    totalDataChart: IChartData,
    totalDataChartBreakdown: IChartDataByDex,
    protocols: ProtocolsResponse[]
    allChains: string[]
}

export type ProtocolStats = (NonNullable<ProtocolAdaptor['protocolsData']>[string] & IGeneralStats)

export const DEFAULT_CHART_BY_ADAPTOR_TYPE: IJSON<AdaptorRecordType> = {
    [AdapterType.VOLUME]: AdaptorRecordType.dailyVolumeRecord,
    [AdapterType.FEES]: AdaptorRecordType.dailyFeesRecord
}

export const DEFAULT_ATTRS_BY_ADAPTOR_TYPE: IJSON<AdaptorRecordType[]> = {
    [AdapterType.VOLUME]: [AdaptorRecordType.dailyVolumeRecord],
    [AdapterType.FEES]: [AdaptorRecordType.dailyFeesRecord, AdaptorRecordType.dailyRevenueRecord]
}

// -> /overview/volumes
// -> /overview/volumes/ethereum
// -> /overview/fees/
// -> /overview/fees/ethereum
export const handler = async (event: AWSLambda.APIGatewayEvent, enableAlerts: boolean = false): Promise<IResponse> => {
    const pathChain = event.pathParameters?.chain?.toLowerCase()
    const adaptorType = event.pathParameters?.type?.toLowerCase()
    const excludeTotalDataChart = event.queryStringParameters?.excludeTotalDataChart?.toLowerCase() === 'true'
    const excludeTotalDataChartBreakdown = event.queryStringParameters?.excludeTotalDataChartBreakdown?.toLowerCase() === 'true'
    const chainFilter = pathChain ? decodeURI(pathChain) : pathChain

    if (!adaptorType) throw new Error("Missing parameter")

    // Import data list
    const adaptorsData = loadAdaptorsData(adaptorType as AdapterType)

    const results = await allSettled(adaptorsData.default.filter(va => va.config?.enabled).map(async (adapter) => {
        const allSumaries = await Promise.all(DEFAULT_ATTRS_BY_ADAPTOR_TYPE[adaptorType].map(async recordType => {
            const summary = await generateProtocolAdaptorSummary(adapter, recordType, chainFilter, async (e) => {
                console.error(e)
                // TODO, move error handling to rejected promises
                if (enableAlerts)
                    await sendDiscordAlert(e.message)
            })
            return {
                recordType,
                summary
            }
        }))
        return allSumaries.reduce<ProtocolAdaptorSummaryCombined>((acc, { recordType, summary }) => {
            const aa = acc
            return {
                name: summary.name,
                disabled: summary.disabled,
                displayName: summary.displayName,
                module: summary.module,
                config: summary.config,
                chains: summary.chains,
                change_1d: {
                    ...(acc.change_1d ? acc.change_1d : {}),
                    [recordType]: summary.change_1d
                },
                change_7d: {
                    ...(acc.change_7d ? acc.change_7d : {}),
                    [recordType]: summary.change_7d
                },
                change_1m: {
                    ...(acc.change_1m ? acc.change_1m : {}),
                    [recordType]: summary.change_1m
                },
                total24h: {
                    ...(acc.total24h ? acc.total24h : {}),
                    [recordType]: summary.total24h
                },
                breakdown24h: {
                    ...(acc.breakdown24h ? acc.breakdown24h : {}),
                    [recordType]: summary.breakdown24h
                },
                protocolsStats: {
                    ...(acc.protocolsStats ? acc.protocolsStats : {}),
                    [recordType]: summary.protocolsStats
                },
                records: {
                    ...(acc.records ? acc.records : {}),
                    [recordType]: summary.records
                },
                recordsMap: {
                    ...(acc.recordsMap ? acc.recordsMap : {}),
                    [recordType]: summary.recordsMap
                },
            }
        }, {} as ProtocolAdaptorSummaryCombined)
    }))

    // Handle rejected dexs
    const rejectedDexs = results.filter(d => d.status === 'rejected').map(fd => fd.status === "rejected" ? fd.reason : undefined)
    rejectedDexs.forEach(console.error)


    const okProtocols = results.map(fd => fd.status === "fulfilled" && fd.value.disabled !== undefined ? fd.value : undefined).filter(d => d !== undefined) as ProtocolAdaptorSummaryCombined[]
    const generalStats = getSumAllDexsToday(okProtocols.map(substractSubsetVolumes))

    let protocolsResponse: IGetOverviewResponseBody['protocols']
    let totalDataChartResponse: IGetOverviewResponseBody['totalDataChart']
    let totalDataChartBreakdownResponse: IGetOverviewResponseBody['totalDataChartBreakdown']

    if (chainFilter) {
        totalDataChartResponse = excludeTotalDataChart ? [] : generateAggregatedVolumesChartData(okProtocols)
        totalDataChartBreakdownResponse = excludeTotalDataChartBreakdown ? [] : generateByDexVolumesChartData(okProtocols)
        protocolsResponse = okProtocols.map(removeVolumesObject)
    } else {
        totalDataChartResponse = excludeTotalDataChart ? [] : generateAggregatedVolumesChartData(okProtocols)
        totalDataChartBreakdownResponse = excludeTotalDataChartBreakdown ? [] : generateByDexVolumesChartData(okProtocols)
        protocolsResponse = okProtocols.map(removeVolumesObject)
    }

    totalDataChartResponse = totalDataChartResponse.slice(totalDataChartResponse.findIndex(it => it[1] !== 0))
    const sumBreakdownItem = (item: { [chain: string]: number }) => Object.values(item).reduce((acc, current) => acc += current, 0)
    totalDataChartBreakdownResponse = totalDataChartBreakdownResponse.slice(totalDataChartBreakdownResponse.findIndex(it => sumBreakdownItem(it[1]) !== 0))

    return successResponse({
        totalDataChart: totalDataChartResponse,
        totalDataChartBreakdown: totalDataChartBreakdownResponse,
        protocols: protocolsResponse,
        allChains: getAllChainsUniqueString(okProtocols.reduce(((acc, protocol) => ([...acc, ...protocol.chains])), [] as string[])),
        ...generalStats,
    } as IGetOverviewResponseBody, 10 * 60); // 10 mins cache
};

const substractSubsetVolumes = (dex: ProtocolAdaptorSummaryCombined, _index: number, dexs: ProtocolAdaptorSummaryCombined[], baseTimestamp?: number): ProtocolAdaptorSummaryCombined => {
    const includedVolume = dex.config?.includedVolume
    if (includedVolume && includedVolume.length > 0) {
        const includedSummaries = dexs.filter(dex => {
            const volumeAdapter = dex.module
            if (!volumeAdapter) throw Error("No volumeAdapter found")
            includedVolume.includes(volumeAdapter)
        })
        let computedSummary: ProtocolAdaptorSummaryCombined = dex
        for (const includedSummary of includedSummaries) {
            const newSum = getSumAllDexsToday([computedSummary], includedSummary, baseTimestamp)
            const recordTypes = Object.keys(newSum)
            computedSummary = {
                ...includedSummary,
                total24h: recordTypes.reduce((acc, rt) => ({ ...acc, [rt]: newSum[rt]['total24h'] }), {}),
                change_1d: recordTypes.reduce((acc, rt) => ({ ...acc, [rt]: newSum[rt]['change_1d'] }), {}),
                change_7d: recordTypes.reduce((acc, rt) => ({ ...acc, [rt]: newSum[rt]['change_7d'] }), {}),
                change_1m: recordTypes.reduce((acc, rt) => ({ ...acc, [rt]: newSum[rt]['change_1m'] }), {}),
            }
        }
        return computedSummary
    }
    else
        return dex
}

type WithOptional<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>;

const removeVolumesObject = (protocol: WithOptional<ProtocolAdaptorSummary, KeysToRemove>): ProtocolsResponse => {
    delete protocol['records']
    delete protocol['module']
    delete protocol['config']
    delete protocol['recordsMap']
    return protocol
}

export const removeEventTimestampAttribute = (v: AdaptorRecord) => {
    delete v.data['eventTimestamp']
    return v
}

/* const getAllChainsUnique = (dexs: VolumeSummaryDex[]) => {
    const allChainsNotUnique = dexs.reduce((acc, { chains }) => chains !== null ? acc.concat(...chains) : acc, [] as string[])
    return allChainsNotUnique.filter((value, index, self) => {
        return self.indexOf(value) === index;
    })
} */

export const getAllChainsUniqueString = (chains: string[]) => {
    return chains.map(formatChain).filter((value, index, self) => {
        return self.indexOf(value) === index;
    })
}

export default wrap(handler);