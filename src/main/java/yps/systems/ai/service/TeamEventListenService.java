package yps.systems.ai.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import yps.systems.ai.model.Team;
import yps.systems.ai.object.TeamPerson;
import yps.systems.ai.repository.ITeamRepository;

import java.util.Collections;
import java.util.Optional;

@Service
public class TeamEventListenService {

    private final ITeamRepository teamRepository;

    @Autowired
    public TeamEventListenService(ITeamRepository teamRepository) {
        this.teamRepository = teamRepository;
    }

    @KafkaListener(topics = "${env.kafka.topicEvent}")
    public void listen(@Payload String payload, @Header("eventType") String eventType, @Header("source") String source) {
        System.out.println("Processing " + eventType + " event from " + source);
        switch (eventType) {
            case "CREATE_TEAM":
                try {
                    Team team = new ObjectMapper().readValue(payload, Team.class);
                    teamRepository.save(team);
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "SET_STUDENT":
                try {
                    TeamPerson teamPerson = new ObjectMapper().readValue(payload, TeamPerson.class);
                    Optional<Team> optionalTeam = teamRepository.findById(teamPerson.teamElementId());
                    optionalTeam.ifPresent(team -> {
                        team.getStudentsElementId().add(teamPerson.personElementId());
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "SET_LEADER":
                try {
                    TeamPerson teamPerson = new ObjectMapper().readValue(payload, TeamPerson.class);
                    Optional<Team> optionalTeam = teamRepository.findById(teamPerson.teamElementId());
                    optionalTeam.ifPresent(team -> {
                        team.setLeaderElementId(teamPerson.personElementId());
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "REMOVE_STUDENT":
                try {
                    TeamPerson teamPerson = new ObjectMapper().readValue(payload, TeamPerson.class);
                    Optional<Team> optionalTeam = teamRepository.findById(teamPerson.teamElementId());
                    optionalTeam.ifPresent(team -> {
                        team.getStudentsElementId().remove(teamPerson.personElementId());
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "REMOVE_LEADER":
                try {
                    TeamPerson teamPerson = new ObjectMapper().readValue(payload, TeamPerson.class);
                    Optional<Team> optionalTeam = teamRepository.findById(teamPerson.teamElementId());
                    optionalTeam.ifPresent(team -> {
                        team.setLeaderElementId(null);
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "REMOVE_PEOPLE":
                try {
                    TeamPerson teamPerson = new ObjectMapper().readValue(payload, TeamPerson.class);
                    Optional<Team> optionalTeam = teamRepository.findById(teamPerson.teamElementId());
                    optionalTeam.ifPresent(team -> {
                        team.setStudentsElementId(Collections.emptyList());
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "UPDATE_TEAM":
                try {
                    Team team = new ObjectMapper().readValue(payload, Team.class);
                    Optional<Team> optionalTeam = teamRepository.findById(team.getId());
                    optionalTeam.ifPresent(existingTeam -> {
                        team.setLeaderElementId(existingTeam.getLeaderElementId());
                        team.setStudentsElementId(existingTeam.getStudentsElementId());
                        teamRepository.save(team);
                    });
                } catch (JsonProcessingException e) {
                    System.err.println("Error parsing Person JSON: " + e.getMessage());
                }
                break;
            case "REMOVE_TEAM":
                Optional<Team> teamOptional = teamRepository.findById(payload.replaceAll("\"", ""));
                teamOptional.ifPresent(value -> teamRepository.deleteById(value.getId()));
                break;
            default:
                System.out.println("Unknown event type: " + eventType);
        }
    }

}
