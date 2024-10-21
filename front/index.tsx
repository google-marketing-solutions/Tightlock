"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { AlertCircle, CheckCircle2 } from "lucide-react";

export default function TightlockInterface() {
  const [ipAddress, setIpAddress] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [connectionName, setConnectionName] = useState("");
  const [configData, setConfigData] = useState("");
  const [isDryRun, setIsDryRun] = useState(true);
  const [response, setResponse] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isError, setIsError] = useState(false);

  const handleSubmit = async (action: string) => {
    setIsLoading(true);
    setIsError(false);
    setResponse("");

    const baseUrl = `http://${ipAddress}/api/v1`;
    const headers = {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    };

    try {
      let result;
      switch (action) {
        case "testConnection":
          result = await fetch(`${baseUrl}/connect`, {
            method: "POST",
            headers,
          });
          break;
        case "createConfig":
          result = await fetch(`${baseUrl}/configs`, {
            method: "POST",
            headers,
            body: configData,
          });
          break;
        case "getConfig":
          result = await fetch(`${baseUrl}/configs:getLatest`, { headers });
          break;
        case "triggerConnection":
          result = await fetch(
            `${baseUrl}/activations/${connectionName}:trigger`,
            {
              method: "POST",
              headers,
              body: JSON.stringify({ dry_run: isDryRun ? 1 : 0 }),
            }
          );
          break;
        default:
          throw new Error("Invalid action");
      }

      if (!result.ok) throw new Error("API request failed");

      const data = await result.json();
      setResponse(JSON.stringify(data, null, 2));
    } catch (error) {
      console.error("Error:", error);
      setIsError(true);
      setResponse("An error occurred while processing your request.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="container mx-auto p-4">
      <Card>
        <CardHeader>
          <CardTitle>Tightlock API Interface</CardTitle>
          <CardDescription>Interact with the Tightlock API</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid w-full items-center gap-4">
            <div className="flex flex-col space-y-1.5">
              <Label htmlFor="ipAddress">Tightlock IP Address</Label>
              <Input
                id="ipAddress"
                value={ipAddress}
                onChange={(e) => setIpAddress(e.target.value)}
                placeholder="Enter IP address"
              />
            </div>
            <div className="flex flex-col space-y-1.5">
              <Label htmlFor="apiKey">API Key</Label>
              <Input
                id="apiKey"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                type="password"
                placeholder="Enter API key"
              />
            </div>
            <div className="flex flex-col space-y-1.5">
              <Label htmlFor="connectionName">Connection Name</Label>
              <Input
                id="connectionName"
                value={connectionName}
                onChange={(e) => setConnectionName(e.target.value)}
                placeholder="Enter connection name"
              />
            </div>
            <div className="flex flex-col space-y-1.5">
              <Label htmlFor="configData">Configuration Data</Label>
              <Textarea
                id="configData"
                value={configData}
                onChange={(e) => setConfigData(e.target.value)}
                placeholder="Enter JSON configuration data"
              />
            </div>
            <div className="flex items-center space-x-2">
              <Switch
                id="dryRun"
                checked={isDryRun}
                onCheckedChange={setIsDryRun}
              />
              <Label htmlFor="dryRun">Dry Run</Label>
            </div>
          </div>
        </CardContent>
        <CardFooter className="flex justify-between">
          <Button onClick={() => handleSubmit("testConnection")}>
            Test Connection
          </Button>
          <Button onClick={() => handleSubmit("createConfig")}>
            Create Config
          </Button>
          <Button onClick={() => handleSubmit("getConfig")}>
            Get Current Config
          </Button>
          <Button onClick={() => handleSubmit("triggerConnection")}>
            Trigger Connection
          </Button>
        </CardFooter>
      </Card>
      {response && (
        <Card className="mt-4">
          <CardHeader>
            <CardTitle className="flex items-center">
              {isError ? (
                <AlertCircle className="mr-2 h-4 w-4 text-red-500" />
              ) : (
                <CheckCircle2 className="mr-2 h-4 w-4 text-green-500" />
              )}
              Response
            </CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="whitespace-pre-wrap break-words">{response}</pre>
          </CardContent>
        </Card>
      )}
      {isLoading && <p className="mt-4 text-center">Loading...</p>}
    </div>
  );
}