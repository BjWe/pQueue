package pQueue;


import callhandling.dataaccesslayer.events.GroupAssignmentsChangedEventTrigger;
import callhandling.dataaccesslayer.events.UserAssignmentsChangedEventTrigger;
import de.starface.bo.GroupBusinessObject;
import de.starface.bo.PhoneBusinessObject;
import de.starface.bo.UserBusinessObject;
import de.starface.bo.callhandling.actions.ModuleBusinessObject;
import de.starface.callhandling.model.callerid.CallerIdContainer;
import de.starface.ch.processing.bo.api.pojo.data.PojoCall;
import de.starface.core.component.StarfaceComponentProvider;
import de.starface.domainobjects.mutables.SkillBean;
import de.starface.integration.uci.java.v30.exceptions.UciException;
import de.starface.middleware.skill.SkillMiddleware;
import de.vertico.starface.StarfaceDataSource;
import de.vertico.starface.module.core.model.VariableType;
import de.vertico.starface.module.core.model.Visibility;
import de.vertico.starface.module.core.runtime.IAGIJavaExecutable;
import de.vertico.starface.module.core.runtime.IAGIRuntimeEnvironment;
import de.vertico.starface.module.core.runtime.VariableScope;
import de.vertico.starface.module.core.runtime.annotations.Function;
import de.vertico.starface.module.core.runtime.annotations.InputVar;
import de.vertico.starface.module.core.runtime.annotations.OutputVar;
import de.vertico.starface.module.core.runtime.functions.entities.GetUsersOfGroup2;
import de.vertico.starface.persistence.connector.GroupHandler;
import de.vertico.starface.persistence.connector.events.triggers.GroupDataSavedEventTrigger;
import de.vertico.starface.persistence.databean.core.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Function(visibility=Visibility.Private, rookieFunction=true, description="Default")
public class PQueueCallEntry implements IAGIJavaExecutable {

    final String classVersion = "1.0.1";

    @InputVar(label="Main Group", description="Hauptgruppe", type=VariableType.STARFACE_GROUP)
    public int groupId;

    @InputVar(label="Target numbers", description="List of target numbers", type=VariableType.MAP)
    public Map<String, String> targetNumbers;

    @InputVar(label="Target name", description="Name of target", type=VariableType.STRING)
    public String targetName;

    @InputVar(label="Placement Groups Prefix", description="Prefix for placement groups", type=VariableType.STRING)
    public String placementGroupsPrefix;

    @InputVar(label="CallerName", description="Name of caller", type=VariableType.STRING)
    public String callerName;

    @InputVar(label="CallerNumber", description="Number of caller", type=VariableType.STRING)
    public String callerNumber;

    @InputVar(label="CallerChannel", description="Channel of caller", type=VariableType.STRING)
    public String callerChannel;

    @InputVar(label="Skill set", description="List of skills", type=VariableType.MAP)
    public Map<String, String> skillSet;

    @InputVar(label="Default timeout", description="Default timeout", type=VariableType.NUMBER)
    public int defaultTimeout;

    @InputVar(label="What if", description="What if", type=VariableType.BOOLEAN)
    public boolean whatIf = false;

    @OutputVar(label="pQueueSuccess", description="True if connection was established")
    public boolean qQueueSuccess = false;

    StarfaceComponentProvider componentProvider = StarfaceComponentProvider.getInstance();

    private Map<String,List<Pair<List<String>, Integer>>> parseSkills(Map<String, String> skillSet, Map<String, String> targetMap, List<SkillBean> systemSkills, Logger log) {
        Map<String,List<Pair<List<String>, Integer>>> skills = new HashMap<>();

        // iterate over all central targets
        for (Map.Entry<String, String> entry : skillSet.entrySet()) {
            // check if target exists
            if(!targetMap.containsValue(entry.getKey())) {
                log.warn("Target {} not found for skill {}", entry.getKey(), entry.getValue());
                continue;
            }

            // split sequential skills
            List<String> sequential = Arrays.asList(entry.getValue().split(";"));
            for(String parallel : sequential) {
                // prepare list for sequential results
                List<String> sequentialResults = new ArrayList<>();
                int timeout = defaultTimeout;

                List<String> parallelConf = Arrays.asList(parallel.split("@"));
                String parallelStr = parallelConf.get(0);

                if(parallelConf.size() > 1) {
                    try {
                        timeout = Integer.parseInt(parallelConf.get(1));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid timeout value: {}", parallelConf.get(1));
                        timeout = defaultTimeout;
                    }
                }

                List<String> skillNames = Arrays.asList(parallelStr.split(","));
                for(String skillName: skillNames) {

                    // check if skill exists
                    // skip wildcard skills
                    if(!skillName.equals("*") && !skillName.equals("?")) {
                        boolean found = false;

                        for (SkillBean systemSkill : systemSkills) {
                            if (systemSkill.getName().equals(skillName)) {
                                found = true;
                                break;
                            }
                        }

                        if(!found) {
                            log.warn("Skill {} not found in systemSkills", skillName);
                            continue;
                        }
                    }

                    sequentialResults.add(skillName);
                }

                if(sequentialResults.isEmpty()) {
                    log.warn("No valid skills found for {}", parallelStr);
                    continue;
                }

                if(!skills.containsKey(entry.getKey())) {
                    List<Pair<List<String>, Integer>> initList = new ArrayList<>();
                    initList.add(new ImmutablePair<>(sequentialResults, timeout));
                    skills.put(entry.getKey(), initList);
                } else {
                    skills.get(entry.getKey()).add(new ImmutablePair<>(sequentialResults, timeout));
                }
            }
        }
        return skills;
    }

    private Map<Integer, List<String>> getUserSkills(List<Integer> userAccountIds, UserBusinessObject ubo, Logger log) {
        Map<Integer, List<String>> userSkills = new HashMap<>();
        for(int userid : userAccountIds) {
            ExtendedUserData user = ubo.getUserByAccountId(userid);
            log.debug("User: {} {} {}", userid, user.getFirstName(), user.getFamilyName());

            List<SkillBean> skills = ubo.getSkillsForUser(user.getAccountId());
            log.trace("Skill count: {}", skills.size());
            List<String> skillNames = new ArrayList<>();
            for(SkillBean skill : skills) {
                log.trace("Skill: {}", skill.getName());
                skillNames.add(skill.getName());
            }
            userSkills.put(userid, skillNames);
        }
        return userSkills;
    }

    protected List<ExtendedGroup> getUsableGroupIds(GroupBusinessObject gbo, String lookup, Logger log){
        List<ExtendedGroup> groups = gbo.getGroupList(false, "", "", lookup, "", true);

        List<ExtendedGroup> result = new ArrayList<>();

        // walk through all groups
        for (ExtendedGroup group : groups) {
            // check if group is usable
            log.trace("checking group: {} {}", group.getId(), group.getDescription());
            if (group.getDescription().startsWith(lookup)) {
                result.add(group);
            }
        }

        return result;
    }

    /*
    protected boolean dbIsMemberOfGroup(Connection con, int userAccountId, int groupId, Logger log) throws SQLException {
        String sql = "SELECT COUNT(*) as c FROM account2parent WHERE accountid = ? AND parentid = ?";
        try (PreparedStatement ps = con.prepareStatement(sql);){
            ps.setInt(1, userAccountId);
            ps.setInt(2, groupId);
            ResultSet rs = ps.executeQuery();
            rs.first();
            return rs.getInt("c") > 0;
        }
    }

    protected List<Integer> dbGetMemberIdsOfGroup(Connection con, int groupId, Logger log) throws SQLException {
        log.trace("Get db members of group {}", groupId);
        List<Integer> result = new ArrayList<>();
        String sql = "SELECT accountid FROM account2parent WHERE parentid = ?";
        try (PreparedStatement ps = con.prepareStatement(sql);){
            ps.setInt(1, groupId);
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                log.trace("Found member: {}", rs.getInt("accountid"));
                result.add(rs.getInt("accountid"));
            }
        }
        return result;
    }

    protected void dbAddMemberToGroup(Connection con, int userAccountId, int groupId, Logger log) throws SQLException {
        log.trace("Add db user {} to group {}", userAccountId, groupId);
        String sql = "INSERT INTO account2parent (accountid, parentid, loggedon, position) VALUES (?, ?, 'false', 0)";
        try (PreparedStatement ps = con.prepareStatement(sql);){
            ps.setInt(1, userAccountId);
            ps.setInt(2, groupId);
            ps.executeUpdate();
        }
    }

    protected void dbLogoffAllMemberFromGroup(Connection con, int groupId, Logger log) throws SQLException {
        log.trace("Logoff all db members from group {}", groupId);
        String sql = "UPDATE account2parent SET loggedon = 'false' WHERE parentid = ?";
        try (PreparedStatement ps = con.prepareStatement(sql);){
            ps.setInt(1, groupId);
            ps.executeUpdate();
        }
    }

    protected void dbLogonMembersToGroup(Connection con, List<Integer> userAccountIds, int groupId, Logger log) throws SQLException {
        log.trace("Logon db members to group {}", groupId);
        /*
         Merkwürdig, dass das nicht funktioniert - Array mit Ints wirft typenfehler

        Integer[] accountIdsArray = userAccountIds.toArray(new Integer[0]);

        String sql = "UPDATE account2parent SET loggedon = 'true' WHERE accountid IN (?) AND parentid = ?";
        try (PreparedStatement ps = con.prepareStatement(sql);){
            ps.setArray(1, con.createArrayOf("Integer", accountIdsArray));
            ps.setInt(2, groupId);
            ps.executeUpdate();
        }

        *-/

        StringBuilder sql = new StringBuilder("UPDATE account2parent SET loggedon = 'true' WHERE parentid = ? AND accountid IN (");
        for (int i = 0; i < userAccountIds.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");

        try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
            // Set the parent ID parameter first
            ps.setInt(1, groupId);

            // Set the account ID parameters
            int index = 2;
            for (Integer accountId : userAccountIds) {
                ps.setInt(index++, accountId);
            }

            // Print the SQL and parameters for debugging
            System.out.println("Executing SQL: " + ps.toString());

            // Execute the update
            ps.executeUpdate();
        }

    }

    protected void triggerGroupChanges(GroupAssignmentsChangedEventTrigger gaTrigger, GroupDataSavedEventTrigger gdTrigger, HashSet<Integer> userIds, int groupId, Logger log) {
        log.trace("Trigger group changes for userIds {}", userIds);
        gaTrigger.sendGroupAssignmentsSettingsChangedEvents(userIds);
        gdTrigger.sendGroupDataChangedEvent(groupId);
    }
    */
    /*
    protected boolean doStuffToRegisterAndLogonUserInGroup(GroupAssignmentsChangedEventTrigger gaTrigger, GroupDataSavedEventTrigger gdTrigger, Connection con, List<Integer> userIds, int groupId, Logger log) {
        log.trace("Do stuff to register and logon users in group {}", groupId);
        try{
            List<Integer> existingUserIds = dbGetMemberIdsOfGroup(con, groupId, log);
            for(int userId : userIds) {
                if(!existingUserIds.contains(userId)) {
                    dbAddMemberToGroup(con, userId, groupId, log);
                }
            }

            dbLogonMembersToGroup(con, userIds, groupId, log);

        } catch (SQLException e) {
            log.error("Error while working with members of group", e);
            return false;
        }

        triggerGroupChanges(gaTrigger, gdTrigger, new HashSet<>(userIds), groupId, log);


        return true;
    }

    protected boolean doStuffToLogoffAllUsersInGroup(GroupAssignmentsChangedEventTrigger gaTrigger, GroupDataSavedEventTrigger gdTrigger, Connection con, List<Integer> userIds, int groupId, Logger log) {
        log.trace("Do stuff to logoff all users in group {}", groupId);
        try{
            dbLogoffAllMemberFromGroup(con, groupId, log);
        } catch (SQLException e) {
            log.error("Error while working with members of group", e);
            return false;
        }

        triggerGroupChanges(gaTrigger, gdTrigger, new HashSet<>(userIds), groupId, log);

        return true;
    }

    */

    protected boolean doStuffToRegisterAndLogonUserInGroup(GroupHandler gh, List<Integer> userIds, int groupId, Logger log) {
        log.trace("Do stuff to register and logon users in group {}", groupId);

        List<Integer> existingUserIds = gh.getAccountsForGroup(groupId);
        List<Integer> usersToAdd = new ArrayList<>();
        for(int userId : userIds) {
            if(!existingUserIds.contains(userId)) {
                usersToAdd.add(userId);
            }
        }

        if(!gh.changeMemberAssignmentsForGroup(groupId, usersToAdd, new ArrayList<Integer>(), new ArrayList<GroupUserSetting>())){
            return false;
        }

        List<GroupUserSetting> settings = gh.getGroupUserSettingListForGroupId(groupId);

        for(GroupUserSetting setting : settings) {
            setting.setAvailable(usersToAdd.contains(setting.getUserData().getAccountId()));
        }
        gh.saveGroupUserSettings(settings, true);

        return true;
    }

    protected boolean doStuffToLogoffAllUsersInGroup(GroupHandler gh, List<Integer> userIds, int groupId, Logger log) {
        log.trace("Do stuff to logoff all users in group {}", groupId);

        List<GroupUserSetting> settings = gh.getGroupUserSettingListForGroupId(groupId);

        for(GroupUserSetting setting : settings) {
            setting.setAvailable(false);
        }
        gh.saveGroupUserSettings(settings, true);

        return true;
    }

    /*
    protected void addUsersToGroup(GroupHandler gh, List<Integer> userAccountIds, int groupId, Logger log) {
        for(int userAccountId : userAccountIds) {
            gh.is

            log.trace("Add user {} to group {}", userAccountId, groupId);

        }
        gh.changeMemberAssignmentsForGroup(groupId, userAccountIds, new ArrayList<Integer>(), new ArrayList<GroupUserSetting>());
    }

    protected void removeUsersFromGroup(GroupHandler gh, List<Integer> userAccountIds, int groupId, Logger log) {
        for(int userAccountId : userAccountIds) {
            log.trace("Remove user {} from group {}", userAccountId, groupId);
        }
        gh.changeMemberAssignmentsForGroup(groupId, new ArrayList<Integer>(), userAccountIds, new ArrayList<GroupUserSetting>());
    }
    */

    private Map<String, Long> getLockedParallelCallGroupsContainer(IAGIRuntimeEnvironment context, Logger log) {
        Map<String, Long> lockedGroups = (Map<String, Long>)context.getScope(VariableScope.Instance).get("lockedParallelCallGroups");
        if(lockedGroups == null) {
            log.info("Create lockedParallelCallGroups");
            lockedGroups = new HashMap<>();
            context.getScope(VariableScope.Instance).put("lockedParallelCallGroups", lockedGroups);
        }
        return lockedGroups;
    }

    protected void lockParallelCallGroups(IAGIRuntimeEnvironment context, Logger log, List<String> parallelCall) {
        log.debug("Lock groups {}", parallelCall);
        synchronized (PQueueCallEntry.class) {
            Map<String, Long> lockedGroups = getLockedParallelCallGroupsContainer(context, log);
            for(String parallelEntry : parallelCall) {
                lockedGroups.put(parallelEntry, System.currentTimeMillis());
            }
        }
    }

    protected void unlockParallelCallGroups(IAGIRuntimeEnvironment context, Logger log, List<String> parallelCall) {
        log.debug("Unlock groups {}", parallelCall);
        synchronized (PQueueCallEntry.class) {
            Map<String, Long> lockedGroups = getLockedParallelCallGroupsContainer(context, log);
            for(String parallelEntry : parallelCall) {
                lockedGroups.remove(parallelEntry);
            }
        }
    }

    protected boolean oneOfParallelCallGroupsLocked(Logger log, IAGIRuntimeEnvironment context, List<String> parallelCall) {
        boolean result = false;

        synchronized (PQueueCallEntry.class) {
            Map<String, Long> lockedGroups = getLockedParallelCallGroupsContainer(context, log);
            for(String parallelEntry : parallelCall) {
                if(lockedGroups.containsKey(parallelEntry)) {
                    Long lockTime = (System.currentTimeMillis() - lockedGroups.get(parallelEntry)) / 1000L;
                    log.debug("Group {} is locked since {} seconds", parallelEntry, lockTime);

                    if(lockTime > (defaultTimeout * 10L)){
                        log.warn("Group {} is locked since {} seconds - unlocking", parallelEntry, lockTime);
                        lockedGroups.remove(parallelEntry);
                    } else {
                        result = true;
                        break;
                    }

                }
            }
        }
        return result;
    }

    private Map<Integer, Long> getLockedStarfaceGroupContainer(IAGIRuntimeEnvironment context, Logger log) {
        Map<Integer, Long> lockedGroups = (Map<Integer, Long>)context.getScope(VariableScope.Instance).get("lockedStarfaceGroups");
        if(lockedGroups == null) {
            log.info("Create lockedStarfaceGroups");
            lockedGroups = new HashMap<>();
            context.getScope(VariableScope.Instance).put("lockedStarfaceGroups", lockedGroups);
        }
        return lockedGroups;
    }

    protected void lockStarfaceGroup(IAGIRuntimeEnvironment context, int groupId, Logger log) {
        log.debug("Lock group {}", groupId);
        synchronized (PQueueCallEntry.class) {
            Map<Integer, Long> lockedGroups = getLockedStarfaceGroupContainer(context, log);
            lockedGroups.put(groupId, System.currentTimeMillis());
        }
    }

    protected void unlockStarfaceGroup(IAGIRuntimeEnvironment context, int groupId, Logger log) {
        log.debug("Unlock group {}", groupId);
        synchronized (PQueueCallEntry.class) {
            Map<Integer, Long> lockedGroups = getLockedStarfaceGroupContainer(context, log);
            lockedGroups.remove(groupId);
        }
    }

    protected boolean isStarfaceGroupLocked(IAGIRuntimeEnvironment context, int groupId, Logger log) {
        synchronized (PQueueCallEntry.class) {
            Map<Integer, Long> lockedGroups = getLockedStarfaceGroupContainer(context, log);
            if (lockedGroups.containsKey(groupId)) {
                Long lockTime = (System.currentTimeMillis() - lockedGroups.get(groupId)) / 1000L;
                log.trace("Group {} is locked since {} seconds", groupId, lockTime);
                if(lockTime > (defaultTimeout * 10L)){
                    log.warn("Group {} is locked since {} seconds - unlocking", groupId, lockTime);
                    lockedGroups.remove(groupId);
                    return false;
                } else {
                    return true;
                }

            }
        }
        return false;
    }

    protected int getFirstFreeStarfaceGroupAndLockIt(IAGIRuntimeEnvironment context, List<Integer> groupIds, Logger log) {
        for(int groupId : groupIds) {
            if(!isStarfaceGroupLocked(context, groupId, log)) {
                lockStarfaceGroup(context, groupId, log);
                return groupId;
            }
        }
        return -1;
    }

    @Override
    public void execute(IAGIRuntimeEnvironment context) throws Exception {
        // get logger
        Logger log = context.getLog();

        log.debug("pQueueEntry v{}", classVersion);

        if(groupId <= 0){
            log.error("No groupId found");
            return;
        }

        // get current channel - cancel if not found
        String currentChannel = callerChannel;
        if(currentChannel == null) {
            currentChannel = context.getCallerChannelName();
            if ((currentChannel == null) && !whatIf) {
                log.error("No callerChannel found");
                return;
            }
        }

        log.debug("callerChannel is: {}", currentChannel);
        log.debug("groupId is : {}", groupId);
        log.debug("targetName is : {}", targetName);

        if(!targetNumbers.containsValue(targetName)) {
            log.error("Target {} not found in targetNumbers", targetName);
            return;
        }

        // get module business object
        ModuleBusinessObject mbo = (ModuleBusinessObject)context.provider().fetch(ModuleBusinessObject.class);

        // get group business object
        GroupBusinessObject gbo = (GroupBusinessObject)context.provider().fetch(GroupBusinessObject.class);

        // get group handler
        GroupHandler gh = (GroupHandler)context.provider().fetch(GroupHandler.class);

        // get user business object
        UserBusinessObject ubo = (UserBusinessObject)context.provider().fetch(UserBusinessObject.class);

        // get phone business object
        PhoneBusinessObject pbo = (PhoneBusinessObject)context.provider().fetch(PhoneBusinessObject.class);

        // get skillmiddleware
        SkillMiddleware smw = (SkillMiddleware)context.provider().fetch(SkillMiddleware.class);

        // get group assignments changed event trigger
        GroupAssignmentsChangedEventTrigger groupAssignmentTrigger = (GroupAssignmentsChangedEventTrigger)context.provider().fetch(GroupAssignmentsChangedEventTrigger.class);

        // get user assignments changed event trigger
        GroupDataSavedEventTrigger groupDataSavedEventTrigger  = (GroupDataSavedEventTrigger )context.provider().fetch(GroupDataSavedEventTrigger.class);

        // get sql datasource connection
        StarfaceDataSource dataSource = (StarfaceDataSource)context.provider().fetch(StarfaceDataSource.class);


        // load all workaround groups
        List<ExtendedGroup> systemGroups = getUsableGroupIds(gbo, placementGroupsPrefix, log);
        if(systemGroups.isEmpty()) {
            log.error("No group found for prefix {}", placementGroupsPrefix);
            return;
        }

        for (ExtendedGroup group : systemGroups) {
            log.debug("Group exists: Id: {} AccountId: {} Int: {}", group.getId(), group.getAccountId(), group.getIntPhoneNumber());
        }


        // load all known skills
        List<SkillBean> allSkills = smw.getSkills();
        if(log.getLevel() == org.apache.logging.log4j.Level.TRACE) {
            for (SkillBean skill : allSkills) {
                log.trace("Skill exists: {}", skill.getName());
            }
        }

        // parse skills
        Map<String, List<Pair<List<String>, Integer>>> parsedSkills = parseSkills(skillSet, targetNumbers, allSkills, log);
        // log skills
        if(log.getLevel() == org.apache.logging.log4j.Level.TRACE) {
            for (Map.Entry<String, List<Pair<List<String>, Integer>>> entry : parsedSkills.entrySet()) {
                log.trace("Target: {}", entry.getKey());
                for(Pair<List<String>, Integer> sequential : entry.getValue()) {
                    log.trace("Parallel: {} for {} seconds", sequential.getLeft(), sequential.getRight());
                }
            }
        }

        // get main group
        ExtendedGroup mainGroup = gbo.getGroupByAccountId(groupId);
        if (mainGroup == null) {
            log.error("No group found for groupId {}", groupId);
            return;
        }

        log.trace("groupId found - name is: {}", mainGroup.getDescription());

        // get users of main group
        GetUsersOfGroup2 mainUsers = new GetUsersOfGroup2();
        mainUsers.groupId = mainGroup.getAccountId();
        mainUsers.activeOnly = true;
        mainUsers.excludeDND = true;
        try {
            mainUsers.execute(context);
        } catch (Exception e) {
            log.error("Error while fetching users of group", e);
            return;
        }

        List<Integer> userAccountIds = mainUsers.usersOfGroup;

        // get skills of users
        Map<Integer, List<String>> userSkills = getUserSkills(userAccountIds, ubo, log);

        List<Integer> alreadyTriedUserAccountIds = new ArrayList<>();

        // build list sequential skills to call
        List<Pair<List<String>, Integer>> sequentialListToCall = parsedSkills.get(targetName);
        if(sequentialListToCall == null) {
            log.error("No target entry found for target {}", targetName);
            return;
        }

        // try to call sequential
        for(Pair<List<String>, Integer> sequentialEntry : sequentialListToCall) {
            log.trace("Paralell: {} for {} seconds", sequentialEntry.getLeft(), sequentialEntry.getRight());

            if(!whatIf) {
                if (!mbo.isParkedCallLegUp(callerChannel)) {
                    log.debug("Call is not up anymore");
                    return;
                } else {
                    log.trace("Call is still up");
                }
            }

            List<String> parallelCall = sequentialEntry.getLeft();
            int timeout = sequentialEntry.getRight();

            // build list of users to call
            List<Integer> usersToCall = new ArrayList<>();

            for(String parallelEntry : parallelCall) {
                log.trace("Walk through parallelEntry: {}", parallelEntry);
                if(parallelEntry.equals("*")){
                    log.trace("Wildcard found");
                    //usersToCall.addAll(userAccountIds);
                    //busy check
                    for(int userAccountId : userAccountIds) {
                        if(!mbo.isUserBusy(userAccountId)) {
                            usersToCall.add(userAccountId);
                            alreadyTriedUserAccountIds.add(userAccountId);
                        } else {
                            log.debug("User {} is busy", userAccountId);
                        }
                    }
                    break;
                } else if(parallelEntry.equals("?")) {
                    log.trace("Not tried users found");
                    for(int userAccountId : userAccountIds) {
                        if(!alreadyTriedUserAccountIds.contains(userAccountId) && !mbo.isUserBusy(userAccountId)) {
                            usersToCall.add(userAccountId);
                            alreadyTriedUserAccountIds.add(userAccountId);
                        } else {
                            log.debug("User {} is busy or already tried", userAccountId);
                        }
                    }
                } else {
                    // skill based selection
                    for (Map.Entry<Integer, List<String>> userSkill : userSkills.entrySet()) {
                        for (String skill : userSkill.getValue()) {
                            if (parallelEntry.equals(skill)) {
                                log.trace("Skill {} found for user {}", skill, userSkill.getKey());
                                // buddy check
                                if(!mbo.isUserBusy(userSkill.getKey())) {
                                    usersToCall.add(userSkill.getKey());
                                    alreadyTriedUserAccountIds.add(userSkill.getKey());
                                } else {
                                    log.debug("User {} is busy", userSkill.getKey());
                                }
                            }
                        }
                    }
                }
            }

            if(usersToCall.isEmpty()) {
                log.error("No users found for parallelEntry {}", parallelCall);
                continue;
            }

            int lockParallelTimeout = timeout;
            while(oneOfParallelCallGroupsLocked(log, context, parallelCall) && lockParallelTimeout > 0) {
                log.trace("One of the parallel groups is locked - waiting 2 seconds, remaining timeout: {}", lockParallelTimeout);
                Thread.sleep(2 * 1000L);
                lockParallelTimeout -= 2;
            }

            if(lockParallelTimeout <= 0) {
                log.debug("Timeout while waiting for parallel groups to unlock");
                continue;
            }

            this.lockParallelCallGroups(context, log, parallelCall);
            log.trace("Locked parallel groups: {}", parallelCall);

            // get free group
            int freeGroupId = this.getFirstFreeStarfaceGroupAndLockIt(context, systemGroups.stream().map(Group::getAccountId).collect(Collectors.toList()), log);
            int lockStarfaceGroupTimeout = timeout;
            while(freeGroupId == -1 && lockStarfaceGroupTimeout > 0) {
                log.trace("No free systemgroup found - waiting 2 seconds, remaining timeout: {}", lockStarfaceGroupTimeout);
                Thread.sleep(2 * 1000L);
                freeGroupId = this.getFirstFreeStarfaceGroupAndLockIt(context, systemGroups.stream().map(Group::getAccountId).collect(Collectors.toList()), log);
                lockStarfaceGroupTimeout -= 2;
            }

            // check if timeout
            if(freeGroupId == -1) {
                log.debug("Timeout while waiting for free systemgroup");
                this.unlockParallelCallGroups(context, log, parallelCall);
                continue;
            }

            // put users into group
            //this.addUsersToGroup(gh, usersToCall, freeGroupId, log);
            boolean logonSuccess = this.doStuffToRegisterAndLogonUserInGroup(gh, usersToCall, freeGroupId, log);
            if(!logonSuccess) {
                log.error("Error while adding users to group");
                this.unlockParallelCallGroups(context, log, parallelCall);
                this.unlockStarfaceGroup(context, freeGroupId, log);
                continue;
            }


            int finalFreeGroupId = freeGroupId;
            PhoneNumberBeanLite intPhoneNumberOfUsedGroup = systemGroups.stream().filter(g-> g.getAccountId() == finalFreeGroupId).findFirst().get().getIntPhoneNumber();

            // call phones
            try {
                if(!whatIf) {
                    //mbo.unparkCall(currentChannel);
                    /*
                    PojoCall call = mbo.getPojoCallByChannelName(currentChannel);
                    UUID uuid = call.getCaller().getCallLegId();

                    Set<String> ignoreModuleInstanceIds = this.getIgnoredModules(context);
                    qQueueSuccess = mbo.dialNumber(uuid, currentChannel, intPhoneNumberOfUsedGroup.getPhoneNumber(), this.callerName, this.callerNumber, timeout, ignoreModuleInstanceIds);
                    */

                    Set<String> ignoreModuleInstanceIds = this.getIgnoredModules(context);
                    UUID accessToken = context.getModuleAccessToken();
                    UUID callUuid = mbo.placeCallWithNumber(intPhoneNumberOfUsedGroup.getPhoneNumber(), this.callerName, this.callerNumber, timeout, true, currentChannel, ignoreModuleInstanceIds, accessToken, currentChannel, false);
                    qQueueSuccess = callUuid != null;

                    if(qQueueSuccess) {

                        log.debug("Successfully called workaround group: {}", intPhoneNumberOfUsedGroup.getPhoneNumber());

                        context.setCallerChannelName(mbo.getChannelNameByCallLegId(callUuid));

                        // remove users from group
                        //this.removeUsersFromGroup(gh, usersToCall, freeGroupId, log);
                        this.doStuffToLogoffAllUsersInGroup(gh, usersToCall, freeGroupId, log);

                        this.unlockParallelCallGroups(context, log, parallelCall);
                        this.unlockStarfaceGroup(context, freeGroupId, log);

                        break;
                    } else {

                        log.debug("Not successfully while calling workaround group: {}", intPhoneNumberOfUsedGroup.getPhoneNumber());
                    }
                } else {
                    log.info("Would call workaround group: {} - waiting {} seconds", intPhoneNumberOfUsedGroup.getPhoneNumber(), timeout);
                    Thread.sleep(timeout * 1000L);
                }
            } catch (Exception e) {
                log.error("Error while calling phones", e);
            }

            // remove users from group
            //this.removeUsersFromGroup(gh, usersToCall, freeGroupId, log);
            this.doStuffToLogoffAllUsersInGroup(gh, usersToCall, freeGroupId, log);

            this.unlockParallelCallGroups(context, log, parallelCall);
            this.unlockStarfaceGroup(context, freeGroupId, log);

        }

        if(!qQueueSuccess) {
            log.trace("No successful call");
        } else {
            log.trace("Successful call");
        }

    }

    private Set<String> getIgnoredModules(IAGIRuntimeEnvironment context) {
        String instanceID = context.getInvocationInfo().getModuleInstance().getId();
        Set<String> ignoredIDs = context.getIgnoreModuleInstanceIds().orElseGet(HashSet::new);
        ignoredIDs.add(instanceID);
        return ignoredIDs;
    }

}
