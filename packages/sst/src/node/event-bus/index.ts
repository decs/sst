import { createProxy } from "../util/index.js";

export interface EventBusResources {}

export const EventBus =
  /* @__PURE__ */ createProxy<EventBusResources>("EventBus");

import {
  EventBridgeClient,
  PutEventsCommand,
  PutEventsCommandOutput,
  PutEventsRequestEntry,
} from "@aws-sdk/client-eventbridge";
import { Schema as TypeSchema } from "@typeschema/core";
import { AdapterResolver, Infer, InferIn, validate } from "@typeschema/main";
import { EventBridgeEvent } from "aws-lambda";
import { useLoader } from "../util/loader.js";
import { Config } from "../config/index.js";

// https://github.com/microsoft/TypeScript/issues/47663
import type {} from "zod";

/**
 * PutEventsCommandOutput is used in return type of createEvent, in case the consumer of SST builds
 * their project with declaration files, this is not portable. In order to allow TS to generate a
 * declaration file without reference to @aws-sdk/client-eventbridge, we must re-export the type.
 *
 * More information here: https://github.com/microsoft/TypeScript/issues/47663#issuecomment-1519138189
 */
export { PutEventsCommandOutput };

export function createEventBuilder<
  Bus extends keyof typeof EventBus,
  MetadataFunction extends () => any,
  MetadataSchema extends TypeSchema<AdapterResolver> | undefined
>(input: {
  bus: Bus;
  metadata?: MetadataSchema;
  metadataFn?: MetadataFunction;
}) {
  const client = new EventBridgeClient({});
  const metadataSchema = input.metadata;
  return function event<
    Type extends string,
    Schema extends TypeSchema<AdapterResolver>
  >(type: Type, schema: Schema) {
    type Publish = undefined extends MetadataSchema
      ? (properties: InferIn<Schema>) => Promise<PutEventsCommandOutput>
      : (
          properties: InferIn<Schema>,
          metadata: InferIn<Exclude<MetadataSchema, undefined>>
        ) => Promise<void>;
    async function publish(properties: any, metadata: any) {
      const result = await useLoader(
        "sst.bus.publish",
        async (input: PutEventsRequestEntry[]) => {
          const size = 10;

          const promises: Promise<any>[] = [];
          for (let i = 0; i < input.length; i += size) {
            const chunk = input.slice(i, i + size);
            promises.push(
              client.send(
                new PutEventsCommand({
                  Entries: chunk,
                })
              )
            );
          }
          const settled = await Promise.allSettled(promises);
          const result = new Array<PutEventsCommandOutput>(input.length);
          for (let i = 0; i < result.length; i++) {
            const item = settled[Math.floor(i / 10)];
            if (item.status === "rejected") {
              result[i] = item.reason;
              continue;
            }
            result[i] = item.value;
          }
          return result;
        }
      )({
        // @ts-expect-error
        EventBusName: EventBus[input.bus].eventBusName,
        // @ts-expect-error
        Source: Config.APP,
        Detail: JSON.stringify({
          properties: await validate(schema, properties),
          metadata: await (async () => {
            if (metadataSchema) {
              return await validate(metadataSchema, metadata);
            }

            if (input.metadataFn) {
              return input.metadataFn();
            }
          })(),
        }),
        DetailType: type,
      });
      return result;
    }
    return {
      publish: publish as Publish,
      type,
      $input: {} as InferIn<Schema>,
      $output: {} as Infer<Schema>,
      $metadata: {} as ReturnType<MetadataFunction>,
    };
  };
}

type Event = {
  type: string;
  $output: any;
  $metadata: any;
};

type EventPayload<E extends Event> = {
  type: E["type"];
  properties: E["$output"];
  metadata: E["$metadata"];
  attempts: number;
};

export function EventHandler<Events extends Event>(
  _events: Events | Events[],
  cb: (
    evt: {
      [K in Events["type"]]: EventPayload<Extract<Events, { type: K }>>;
    }[Events["type"]]
  ) => Promise<void>
) {
  return async (
    event: EventBridgeEvent<string, any> & { attempts?: number }
  ) => {
    await cb({
      type: event["detail-type"],
      properties: event.detail.properties,
      metadata: event.detail.metadata,
      attempts: event.attempts ?? 0,
    });
  };
}
