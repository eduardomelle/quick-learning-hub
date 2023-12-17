package br.com.eduardomelle.retryconsumer.repository;

import br.com.eduardomelle.retryconsumer.entity.FailedMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedMessageRepository extends JpaRepository<FailedMessage, Long> {
}
