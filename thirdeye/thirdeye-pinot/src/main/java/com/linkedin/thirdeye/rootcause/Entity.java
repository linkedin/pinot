package com.linkedin.thirdeye.rootcause;

import java.util.Comparator;


/**
 * An entity represents a node in the knowledge graph traversed to identify potential root causes.
 * The URN represents a unique identifier (with a hierarchical namespace) and the score identifies
 * the relative importance to other entities given a specific context.
 * In the user-specified search context, the score represents the subjective importance of the
 * entity as determined by the user. In the execution context and the pipeline results the score
 * represents the relative importance of entities associated with the search context as determined
 * by individual pipelines and the aggregator.
 *
 * <br/><b>NOTE:</b> a subclass of {@code Entity} may be returned by pipelines in order to pass
 * along transient meta-data for individual entities.
 *
 * <br/><b>NOTE:</b> due to the potentially vast number of nodes in the knowledge graph there is no
 * centralized repository of valid URNs.
 */
public class Entity {
  final String urn;
  final double score;

  public static final Comparator<Entity> HIGHEST_SCORE_FIRST = new Comparator<Entity>() {
    @Override
    public int compare(Entity o1, Entity o2) {
      return -Double.compare(o1.score, o2.score);
    }
  };

  public Entity(String urn, double score) {
    this.urn = urn;
    this.score = score;
  }

  public String getUrn() {
    return urn;
  }

  public double getScore() {
    return score;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Entity entity = (Entity) o;

    if (Double.compare(entity.score, score) != 0) {
      return false;
    }
    return urn != null ? urn.equals(entity.urn) : entity.urn == null;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = urn != null ? urn.hashCode() : 0;
    temp = Double.doubleToLongBits(score);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
