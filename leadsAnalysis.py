
from datetime import *
import xlsxwriter
import pandas
import csv

# Andrew Estis
# August 2017
# Improbable.io Remote Interview Task
# Designed for use on Windows with pandas 0.20.3


class clientRecord:
    def __init__(self, id, name, company, city, industry, status):
        self.id = id
        self.name = name
        self.company = company
        self.city = city
        self.industry = industry
        self.status = int(status[0])
        self.interactions = pandas.DataFrame()

    def selfDescribe(self, showInteractions=False):
        print self.id
        print "\t".join((self.name, self.company, self.city, self.industry))
        print self.status
        print "---"

        if showInteractions:
            print self.interactions.head()


def prepareEvents(eventsPath):
    """ Quick method to grab events from csv, convert to pandas, and cleanup.
    :param eventsPath: path to events.csv
    :return: formatted events pandas object
    """
    events = pandas.read_csv(eventsPath, parse_dates=[1])
    events = events.drop_duplicates()
    events = events.set_index(["email", "received_at"])
    events['source_event'] = events['source']+"_"+events["event"]
    return events


def createClients(clientsPath, eventsPath):
    allClients = []
    with open(clientsPath, "rU") as csvFile:
        csvReader = csv.reader(csvFile)
        csvReader.next()  # skip header
        for row in csvReader:
            newClient = clientRecord(*row)
            allClients.append(newClient)

    events = prepareEvents(eventsPath)
    for client in allClients:
        if client.id not in events.index:
            continue
        client.interactions = events.ix[client.id]

    return allClients


def analytics(eventsPath, clients=None):
    events = prepareEvents(eventsPath)

    def eventFrequencies(events, groupbyUU=True):
        eventFreq = {}
        eventTypes = set(events['source_event'])
        for eventType in eventTypes:
            subset = events[events["source_event"] == eventType].reset_index()
            if groupbyUU:
                # Group by unique users by dropping duplicates from 'email' (id)
                subset = subset.drop_duplicates(["email"])
            eventFreq[eventType] = len(subset)

        with open("analysis/eventFrequency.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            for evt in eventFreq:
                csvWriter.writerow((evt, eventFreq[evt]))
        return eventFreq

    def serializeEvents(events):
        userPaths = {}
        allUsers = events.index.get_level_values("email").unique()
        for userid in allUsers:
            subset = events.ix[userid]
            # Get a subset of non-simultaneous events
            userPath = subset.loc[subset.shift()["source_event"] != subset["source_event"]]
            userPath = tuple(userPath["source_event"])

            # Increment dict entry if it exists, don't increment if not.
            userPaths[userPath] = userPaths[userPath] + 1 if userPath in userPaths else 1

        with open("analysis/orderedEvents.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            for path in userPaths:
                csvWriter.writerow((userPaths[path], path))

    def serializeEventsByIndustry(allowRepeats=False):
        userPaths = {}
        allPaths = set()

        for industry in set(client.industry for client in clients):
            userPaths[industry] = {}
            for client in clients:
                subset = client.interactions
                if subset.empty:
                    continue
                if allowRepeats:
                    userPath = subset.loc[subset.shift()["source_event"] != subset["source_event"]]
                else:
                    userPath = subset.drop_duplicates()
                userPath = tuple(userPath["source_event"])
                allPaths.add(userPath)
                # Increment dict entry if it exists, don't increment if not.
                if userPath in userPaths[industry]:
                    userPaths[industry][userPath] += 1
                else:
                    userPaths[industry][userPath] = 1

        with open("analysis/serializedByIndustry.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            csvWriter.writerow([""]+list(industry for industry in userPaths))
            for userPath in allPaths:
                row = [userPath]
                for industry in list(industry for industry in userPaths):
                    if userPath in userPaths[industry]:
                        row.append(userPaths[industry][userPath])
                csvWriter.writerow(row)



    def interactionsPerClient(events):
        clientInteractions = {}
        for client in clients:
            # subset = events[events["email"] == client.id]
            subset = events.ix[client.id] if client.id in events.index.get_level_values("email") else []
            clientInteractions[client] = len(subset)

        with open("analysis/interactionsPerClient.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            for client in clientInteractions:
                csvWriter.writerow((client.id, clientInteractions[client]))

    def outreachStatusSummary():
        statuses = {}
        industries = set(client.industry for client in clients)
        for statusID in range(1, 6):
            statuses[statusID] = {}
            for industryID in industries:
                # changed from status == statusID to '>='
                statuses[statusID][industryID] = len(filter(lambda x: x.status >= statusID and
                                                                      x.industry == industryID, clients))
            statuses[statusID]["Total"] = len(filter(lambda x: x.status >= statusID, clients))

        with open("analysis/outreachStatusSummary.csv","w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            industries = list(industries) + ["Total"]
            csvWriter.writerow([""]+industries)
            for statusID in statuses:
                csvWriter.writerow([statusID] + list(statuses[statusID][industry] for industry in industries))

    def interactionsByIndustry(events, metric="source_event"):
        eventsIndustries = {}
        eventTypes = set(events[metric])

        for event in eventTypes:
            eventsIndustries[event] = {}
            for client in clients:
                if client.interactions.empty:
                    continue
                numEvents = len(client.interactions[client.interactions[metric] == event])
                if client.industry in eventsIndustries[event]:
                    eventsIndustries[event][client.industry] += numEvents
                else:
                    eventsIndustries[event][client.industry] = numEvents

        with open("analysis/interactionsByIndustry.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            csvWriter.writerow([""]+list(eventType for eventType in eventsIndustries))
            for industry in set(client.industry for client in clients):
                row = [industry]
                for event in list(eventType for eventType in eventsIndustries):
                    row.append(eventsIndustries[event][industry])
                csvWriter.writerow(row)

#    eventFrequencies(events, False)
#    print "eventFrequencies completed"
#    serializeEvents(events)
#    print "serializeEvents completed"
#    interactionsPerClient(events)
#    print "interactionsPerClient completed"
#    interactionsByIndustry(events, "source_event")
#    print "interactionsByIndustry completed"
    serializeEventsByIndustry()
    print "serializeEventsByIndustry completed"


def main():
    eventsPath = "data\data\events.csv"
    leadsPath = "data\data\leads.csv"

    clients = createClients(leadsPath, eventsPath)
    # analyzeClients(clients)
    analytics(eventsPath, clients)

if __name__ == "__main__":
    main()