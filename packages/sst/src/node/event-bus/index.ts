import { createProxy } from "../util/index.js";

export interface EventBusResources {}

export const EventBus =
  /* @__PURE__ */ createProxy<EventBusResources>("EventBus");

import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { EventBridgeEvent } from "aws-lambda";
import { Schema, Infer, assert } from "@decs/typeschema";

const client = new EventBridgeClient({});

export function createEventBuilder<
  Bus extends keyof typeof EventBus,
  MetadataShape extends { [k: string]: Schema<any> } | undefined,
  MetadataFunction extends () => any
>(props: {
  bus: Bus;
  metadata?: MetadataShape;
  metadataFn?: MetadataFunction;
}) {
  return function createEvent<
    Type extends string,
    Shape extends { [k: string]: Schema<any> },
    Properties = Infer<Shape>
  >(type: Type, properties: Shape) {
    type Publish = undefined extends MetadataShape
      ? (properties: Properties) => Promise<void>
      : (
          properties: Properties,
          metadata: Infer<Exclude<MetadataShape, undefined>>
        ) => Promise<void>;
    const propertySchemas = properties;
    const metadataSchemas = props.metadata;
    const publish = async (properties: any, metadata: any) => {
      console.log("publishing", type, properties);
      await client.send(
        new PutEventsCommand({
          Entries: [
            {
              // @ts-expect-error
              EventBusName: EventBus[props.bus].eventBusName,
              Source: "console",
              Detail: JSON.stringify({
                properties: await assertMulti(propertySchemas, properties),
                metadata: await (async () => {
                  if (metadataSchemas) {
                    return await assertMulti(metadataSchemas, metadata);
                  }

                  if (props.metadataFn) {
                    return props.metadataFn();
                  }
                })(),
              }),
              DetailType: type,
            },
          ],
        })
      );
    };

    return {
      publish: publish as Publish,
      type,
      shape: {
        metadata: {} as Parameters<Publish>[1],
        properties: {} as Properties,
        metadataFn: {} as ReturnType<MetadataFunction>,
      },
    };
  };
}

type Event = {
  type: string;
  shape: {
    properties: any;
    metadata: any;
    metadataFn: any;
  };
};

type EventPayload<E extends Event> = {
  type: E["type"];
  properties: E["shape"]["properties"];
  metadata: undefined extends E["shape"]["metadata"]
    ? E["shape"]["metadataFn"]
    : E["shape"]["metadata"];
};

export function EventHandler<Events extends Event>(
  _events: Events | Events[],
  cb: (
    evt: {
      [K in Events["type"]]: EventPayload<Extract<Events, { type: K }>>;
    }[Events["type"]]
  ) => Promise<void>
) {
  return async (event: EventBridgeEvent<string, any>) => {
    await cb({
      type: event["detail-type"],
      properties: event.detail.properties,
      metadata: event.detail.metadata,
    });
  };
}

async function assertMulti(
  schemas: { [k: string]: Schema<any> },
  data: { [k: string]: any }
) {
  const values = await Promise.all(
    Object.keys(schemas).map(
      (key) => async () => await assert(schemas[key], data[key])
    )
  );
  return Object.keys(schemas).reduce(
    (others, key, index) => ({
      ...others,
      [key]: values[index],
    }),
    {}
  );
}
