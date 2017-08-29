import xlsxwriter
import pandas
import csv

# Andrew Estis
# August 2017
# Improbable.io Remote Interview Task
# Designed for use on Windows with pandas 0.20.3


class clientRecord:
    """ Class representing an individual client, with all client attributes and actions (events) performed by client.

    """
    def __init__(self, id, name, company, city, industry, status):
        self.id = id
        self.name = name
        self.company = company
        self.city = city
        self.industry = industry
        self.status = int(status[0])
        self.interactions = pandas.DataFrame()

    def selfDescribe(self, showInteractions=False):
        """ Diagnostic self-description function for each client.
        :param showInteractions: If True, include show a subset of the client's interactions.
        :return:
        """
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
    """ Instantiate clientRecord objects, add to list. Prepare events Dataframes and add to Client objects.

    :param clientsPath: Path to leads.csv.
    :param eventsPath: Path to events.csv.
    :return: allClients: A list of Client objects with Events dataframes attached.
    """
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
    """Main driver function for all analytics of lead and event data (from csv).

    :param eventsPath: Path to events.csv.
    :param clients: Path to clients.csv.
    :return: None
    """
    events = prepareEvents(eventsPath)

    def eventFrequencies(events, groupbyUU=True):
        """ Find frequencies at which all events were performed and write to csv.
        :param events: Pandas DataFrame containing events. Passed from prepareEvents().
        :param groupbyUU: Group events by Unique User. If true, repeated actions by a user will not be counted.
        """
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

    def serializeEvents(events):
        """ Find all linear sequences of events performed by unique clients. Count the number of times that
            clients performed events in the same order, and write to csv.
        :param events: Pandas DataFrame containing events. Passed from prepareEvents().
        """
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

    def serializeEventsByIndustry(allowRepeats=False, percentage=False):
        """ Find all linear sequences of events performed by  clients in each industry. Count the number of times that
            clients in a given industry performed events in the same order, and write to csv.
        :param allowRepeats: Ignore repeated events. Event sequence (A, B, B, C) will be recorded a (A, B, C) if true.
        :param percentage: State the number of clients that followed a specific path as percentage of all clients
        in an industry.
        :return:
        """
        userPaths = {}
        allPaths = set()

        for industry in set(client.industry for client in clients):
            userPaths[industry] = {}
            for client in filter(lambda x: x.industry == industry, clients):
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
        if percentage:
            for industry in userPaths:
                for userPath in userPaths[industry]:
                    # Turn the gross number of clients that followed a speciifc path to a percentage of all clients.
                    userPaths[industry][userPath] /= float(len(filter(lambda x: x.industry == industry, clients)))

        with open("analysis/serializedByIndustry.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            csvWriter.writerow([""]+list(industry for industry in userPaths))
            for userPath in allPaths:
                row = [userPath]
                for industry in list(industry for industry in userPaths):
                    if userPath in userPaths[industry]:
                        row.append(userPaths[industry][userPath])
                csvWriter.writerow(row)

    def outreachStatusByInteractions(events, metric="source_event", average=False):
        """ Record the (average) number of interactions of each type per client, and organize by action x outreach status
            (1-5). Create xlsx workbook with one page per industry and one 'overall' page measuring all clients.
        :param events: Pandas DataFrame containing events. Passed from prepareEvents().
        :param metric: The event type to use. Valid: source_event, source, event.
        :param average: Take an average of client interactions instead of the gross number per lead engagement status.
        """
        outreachStatuses = {}
        allInteractions = events.reset_index()

        for industry in list(set(client.industry for client in clients))+["All"]:
            outreachStatuses[industry] = {}
            for outreach in range(1, 6):
                outreachStatuses[industry][outreach] = {}
                # Generate a dataframe of client IDs that match the given outreach status
                emails = pandas.DataFrame((client.id for client in
                                           filter(lambda x: x.status == outreach and x.industry == industry, clients)),
                                          columns=["email"])
                if industry == "All":
                    emails = pandas.DataFrame((client.id for client in
                                               filter(lambda x: x.status == outreach, clients)), columns=["email"])

                emails.set_index("email", inplace=True)
                # Create sub-dataframe only including events for the given outreach status
                joined = allInteractions.join(emails, on="email", how="inner")[metric]

                if not joined.empty:
                    if average:
                        denominator = len(emails)
                        joined = joined.value_counts().to_dict()
                        for event in joined:
                            joined[event] /= float(denominator)
                        outreachStatuses[industry][outreach] = joined
                    else:
                        outreachStatuses[industry][outreach] = joined.value_counts().to_dict()

                # Ensure there is a value for every outreach status (set 0 if no value).
                for x in allInteractions[metric].unique():
                    if x not in outreachStatuses[industry][outreach].keys():
                        outreachStatuses[industry][outreach][x] = 0

        wb = xlsxwriter.Workbook("analysis/statusByInteractions.xlsx")
        for industry in list(set(client.industry for client in clients))+["All"]:
            ws = wb.add_worksheet(industry)
            # ws.write(y,x)

            # headers
            for x, status in enumerate(outreachStatuses[industry]):
                ws.write(0, x+1, status)
            for y, event in enumerate(outreachStatuses[industry][1]):
                ws.write(y+1, 0, event)

            for x, status in enumerate(outreachStatuses[industry]):
                for y, event in enumerate(outreachStatuses[industry][status]):
                    ws.write(y+1, x+1, outreachStatuses[industry][status][event])

        wb.close()

    def interactionsPerClient(events):
        """ Find number of interactions per unique client, organized by client ID.
        :param events: Pandas DataFrame containing events. Passed from prepareEvents().
        """
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
        """ Count the number of clients at or above each outreach status (1-5) for each industry. Record to csv.
        """
        statuses = {}
        industries = set(client.industry for client in clients)
        for statusID in range(1, 6):
            statuses[statusID] = {}
            for industryID in industries:
                # changed from status == statusID to '>='
                statuses[statusID][industryID] = len(filter(lambda x: x.status >= statusID and
                                                                      x.industry == industryID, clients))
            statuses[statusID]["Total"] = len(filter(lambda x: x.status >= statusID, clients))

        with open("analysis/outreachStatusSummary.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            industries = list(industries) + ["Total"]
            csvWriter.writerow([""]+industries)
            for statusID in statuses:
                csvWriter.writerow([statusID] + list(statuses[statusID][industry] for industry in industries))

    def interactionsByIndustry(events, metric="source", average=False):
        """ Count total (average) number of client interactions with each part of the platform (# events) and organize
            by industry.
        :param events: Pandas DataFrame containing events. Passed from prepareEvents().
        :param metric: The event type to use. Valid: source_event, source, event.
        :param average: Take the average number of client interactions per industry, if true.
        """
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

        if average:
            for event in eventsIndustries:
                for industry in eventsIndustries[event]:
                    eventsIndustries[event][industry] /= float(len(filter(lambda x: x.industry == industry, clients)))

        with open("analysis/interactionsByIndustry.csv", "w+") as csvFile:
            csvWriter = csv.writer(csvFile, lineterminator="\n")
            csvWriter.writerow([""]+list(eventType for eventType in eventsIndustries))
            for industry in set(client.industry for client in clients):
                row = [industry]
                for event in list(eventType for eventType in eventsIndustries):
                    row.append(eventsIndustries[event][industry])
                csvWriter.writerow(row)

    eventFrequencies(events, False)
    print "eventFrequencies completed"
    serializeEvents(events)
    print "serializeEvents completed"
    interactionsPerClient(events)
    print "interactionsPerClient completed"
    interactionsByIndustry(events, "source_event", average=False)
    print "interactionsByIndustry completed"
    serializeEventsByIndustry(percentage=True)
    print "serializeEventsByIndustry completed"
    outreachStatusByInteractions(events, average=True)
    print "outreachStatusByInteractions completed"
    outreachStatusSummary()
    print "outreachStatusSummary completed"


def main():
    """ Main driver function.

    """
    eventsPath = "data\data\events.csv"
    leadsPath = "data\data\leads.csv"

    clients = createClients(leadsPath, eventsPath)
    analytics(eventsPath, clients)

if __name__ == "__main__":
    main()
