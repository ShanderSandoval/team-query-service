package yps.systems.ai.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import yps.systems.ai.model.Team;

@Repository
public interface ITeamRepository extends MongoRepository<Team, String> {
}
