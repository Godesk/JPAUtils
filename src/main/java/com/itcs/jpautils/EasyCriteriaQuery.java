package com.itcs.jpautils;

import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;
import org.eclipse.persistence.config.HintValues;
import org.eclipse.persistence.config.QueryHints;

/**
 *
 * @param <T> Entity sobre el que se realizará la query.
 * @author jorge
 */
public class EasyCriteriaQuery<T> implements Iterator<List<T>> {

    private int maxResults;
    private int firstResult;
    private boolean all = true;
//    private final EntityManager entityManager;
    private EntityManagerFactory emf = null;
    private Class<T> entityBeanType;
    private List<T> list;
    private CriteriaBuilder criteriaBuilder;
    private String orderField;
    private boolean orderAsc;
    private List<Object[]> equalList;
    private List<Object[]> distinctList;
    private List<Object[]> likeList;
    private List<PredicateBuilder<T>> betweenList;
    private int lastResult;
    private final LinkedList<PredicateBuilder<T>> emptyList;
    private final LinkedList<PredicateBuilder<T>> notEmptyList;
    private EntityManager entityManager;

    /**
     * Clase que permite crear un CriteriaQuery de forma sencilla utilizando
     * JPA.
     *
     * @param entityManager EntityManager que ejecutará la query.
     * @param entityBeanType Entity sobre el que se realizará la query.
     */
    public EasyCriteriaQuery(EntityManagerFactory entityManagerFactory, Class<T> entityBeanType) {
        this.emf = entityManagerFactory;
        this.entityBeanType = entityBeanType;
//        this.entityBeanType = ((Class) ((ParameterizedType) getClass()
//                .getGenericSuperclass()).getActualTypeArguments()[0]);
        equalList = new LinkedList<Object[]>();
        distinctList = new LinkedList<Object[]>();
        likeList = new LinkedList<Object[]>();
        emptyList = new LinkedList<PredicateBuilder<T>>();
        notEmptyList = new LinkedList<PredicateBuilder<T>>();
        betweenList = new LinkedList<PredicateBuilder<T>>();
    }

    public EasyCriteriaQuery(EntityManager entityManager, Class<T> entityBeanType) {
        this.entityManager = entityManager;
        this.entityBeanType = entityBeanType;
//        this.entityBeanType = ((Class) ((ParameterizedType) getClass()
//                .getGenericSuperclass()).getActualTypeArguments()[0]);
        equalList = new LinkedList<Object[]>();
        distinctList = new LinkedList<Object[]>();
        likeList = new LinkedList<Object[]>();
        emptyList = new LinkedList<PredicateBuilder<T>>();
        notEmptyList = new LinkedList<PredicateBuilder<T>>();
        betweenList = new LinkedList<PredicateBuilder<T>>();
    }

    public EntityManager getEntityManager() {
        if (null == entityManager) {
            return emf.createEntityManager();
        } else {
            return entityManager;
        }
    }

    /**
     *
     * @return Entity sobre el que se realizará la query.
     */
    public Class<T> getEntityBeanType() {
        return entityBeanType;
    }

    /**
     * Agrega condicion equals al predicado(where) de la query
     *
     * @param field nombre del campo
     * @param object valor a comparar
     */
    public void addEqualPredicate(String field, Object object) {
        equalList.add(new Object[]{field, object});
    }

    /**
     * Agrega condicion distinct al predicado(where) de la query
     *
     * @param field nombre del campo
     * @param object valor a comparar
     */
    public void addDistinctPredicate(String field, Object object) {
        distinctList.add(new Object[]{field, object});
    }

    /**
     * Agrega condicion like al predicado(where) de la query
     *
     * @param field nombre del campo
     * @param pattern patron a buscar
     */
    public void addLikePredicate(String field, String pattern) {
        likeList.add(new Object[]{field, pattern});
    }

    /**
     * Agrega condicion between al predicado(where) de la query
     *
     * @param <Y> clase comparable
     * @param field campo a comparar
     * @param object1 objeto limite izquierdo
     * @param object2 objeto limite derecho
     */
    public <Y extends Comparable<? super Y>> void addBetweenPredicate(SingularAttribute<? super T, Y> field, Y object1, Y object2) {
        betweenList.add(new BetweenParams<T, Y>(field, object1, object2));
    }
    
    public <Y extends Comparable<? super Y>> void addLessThanPredicate(SingularAttribute<? super T, Y> field, Y object1, boolean lessOrEqualThan) {
        betweenList.add(new LessThanParams<T, Y>(field, object1, lessOrEqualThan));
    }
    
    public <Y extends Comparable<? super Y>> void addGreaterThanPredicate(SingularAttribute<? super T, Y> field, Y object1, boolean greaterOrEqualThan) {
        betweenList.add(new GreaterThanParams<T, Y>(field, object1, greaterOrEqualThan));
    }

    public <Y, C extends Collection<Y>> void addIsEmptyPredicate(String field) {
        emptyList.add(new EmptyParam<Y, C>(field));
    }

    public <Y, C extends Collection<Y>> void addIsNotEmptyPredicate(String field) {
        notEmptyList.add(new NotEmptyParam<Y, C>(field));
    }

    private Predicate addEqualPredicate(Root<T> root, Predicate predicate, String field, Object object) {
        Predicate localPredicate = getCriteriaBuilder().equal(createExpression(root, field), object);
        return addPredicate(predicate, localPredicate);
    }

    private Expression<?> createExpression(Root<T> root, String fieldPath) {
        Path<T> path = null;
        String[] fieldsList = fieldPath.split("\\.");

        for (String field : fieldsList) {
            if (null == path) {
                path = root.get(field);
            } else {
                path = path.get(field);
            }
        }

        return path;
    }

    private Predicate addDistinctPredicate(Root<T> root, Predicate predicate, String field, Object object) {
        Predicate localPredicate = getCriteriaBuilder().notEqual(createExpression(root, field), object);
        return addPredicate(predicate, localPredicate);
    }

    private Predicate addLikePredicate(Root<T> root, Predicate predicate, String field, String pattern) {
        Predicate localPredicate = getCriteriaBuilder().like(getCriteriaBuilder().upper(((Expression<String>) createExpression(root, field))), pattern.toUpperCase());
        return addPredicate(predicate, localPredicate);
    }

    private Predicate createPredicate(Root<T> root) {
        Predicate predicate = null;
        for (Object[] objArray : equalList) {
            predicate = addEqualPredicate(root, predicate, (String) objArray[0], objArray[1]);
        }
        for (Object[] objArray : distinctList) {
            predicate = addDistinctPredicate(root, predicate, (String) objArray[0], objArray[1]);
        }
        for (Object[] objArray : likeList) {
            predicate = addLikePredicate(root, predicate, (String) objArray[0], (String) objArray[1]);
        }
        for (PredicateBuilder<T> predicateBuilder : betweenList) {
            predicate = addPredicate(predicate, predicateBuilder.build(getCriteriaBuilder(), root));
        }
        for (PredicateBuilder<T> predicateBuilder : emptyList) {
            predicate = addPredicate(predicate, predicateBuilder.build(getCriteriaBuilder(), root));
        }
        for (PredicateBuilder<T> predicateBuilder : notEmptyList) {
            predicate = addPredicate(predicate, predicateBuilder.build(getCriteriaBuilder(), root));
        }

        return predicate;
    }

    private Predicate addPredicate(Predicate predicate, Predicate localPredicate) {
        if (predicate == null) {
            predicate = localPredicate;
        } else {
            predicate = getCriteriaBuilder().and(predicate, localPredicate);
        }
        return predicate;
    }

    private CriteriaBuilder getCriteriaBuilder() {
        EntityManager em = getEntityManager();
        try {
            if (null == this.criteriaBuilder) {
                this.criteriaBuilder = em.getCriteriaBuilder();
            }
            return this.criteriaBuilder;
        } finally {

            closeIfNeeded(em);
        }
    }

    /**
     *
     * @param field
     * @param asc
     */
    public void orderBy(String field, boolean asc) {
        this.orderField = field;
        this.orderAsc = asc;
    }

    private TypedQuery<T> createQuery(EntityManager em) {
        CriteriaQuery cq = createCriteriaQuery();
        Root<T> root = cq.from(entityBeanType);
        if (null != orderField) {
            if (orderAsc) {
                cq.orderBy(getCriteriaBuilder().asc(createExpression(root, orderField)));
            } else {
                cq.orderBy(getCriteriaBuilder().desc(createExpression(root, orderField)));
            }
        }
        Predicate predicate = createPredicate(root);
        if (null != predicate) {
            cq.where(predicate);
        }
        TypedQuery<T> query = em.createQuery(cq);
        if (!all) {
            query.setMaxResults(maxResults);
            query.setFirstResult(firstResult);
        }
        return query;
    }

    /**
     *
     * @return
     */
    public Long count() {
        EntityManager em = getEntityManager();
        try {
            CriteriaQuery cq = createCriteriaQuery();
            Root<T> root = cq.from(entityBeanType);
            Predicate predicate = createPredicate(root);
            if (predicate == null) {
                cq.select(getCriteriaBuilder().count(root));
            } else {
                cq.select(getCriteriaBuilder().count(root)).where(predicate);
            }
            Query q = em.createQuery(cq);
            q.setHint(QueryHints.REFRESH, HintValues.TRUE);
            Long retorno = ((Long) q.getSingleResult()).longValue();
            return retorno;
        } finally {
            closeIfNeeded(em);
        }
    }

    private void closeIfNeeded(EntityManager em) {
        if (entityManager == null) {
            em.close();
        }
    }

    /**
     * @return the criteriaQuery
     */
    private CriteriaQuery<T> createCriteriaQuery() {
        return getCriteriaBuilder().createQuery(entityBeanType);
    }

    /**
     * @return the maxResults
     */
    public int getMaxResults() {
        return maxResults;
    }

    /**
     * @param maxResults the maxResults to set
     */
    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
        this.setAll(false);
    }

    /**
     * @return the firstResult
     */
    public int getFirstResult() {
        return firstResult;
    }

    /**
     * @param firstResult the firstResult to set
     */
    public void setFirstResult(int firstResult) {
        this.firstResult = firstResult;
    }

    /**
     * @return the all
     */
    public boolean isAll() {
        return all;
    }

    /**
     * @param all the all to set
     */
    public void setAll(boolean all) {
        this.all = all;
    }

    /**
     * @param entityBeanType the entityBeanType to set
     */
    public void setEntityBeanType(Class entityBeanType) {
        this.entityBeanType = entityBeanType;
    }

    @Override
    public boolean hasNext() {
        EntityManager em = getEntityManager();
        try {
            if (getLastResult() > 0) {
                if (firstResult >= getLastResult()) {
                    return false;
                }
                if ((firstResult + maxResults) > getLastResult()) {
                    setMaxResults(getLastResult() - firstResult);
                }
            }
            TypedQuery<T> query = createQuery(em);
            query.setHint(QueryHints.REFRESH, HintValues.TRUE);
            list = query.getResultList();
            setFirstResult(firstResult + maxResults);
            return !list.isEmpty();
        } finally {
            closeIfNeeded(em);
        }
    }

    public List<T> getAllResultList() {
        EntityManager em = getEntityManager();
        try {
//            this.all = true;
            TypedQuery<T> query = createQuery(em);
            query.setHint(QueryHints.REFRESH, HintValues.TRUE);
            return query.getResultList();
        } finally {
            closeIfNeeded(em);
        }
    }

    public T getSingleResult() {
        EntityManager em = getEntityManager();
        try {
            this.all = true;
            TypedQuery<T> query = createQuery(em);
            query.setHint(QueryHints.REFRESH, HintValues.TRUE);
            return query.getSingleResult();
        } finally {
            closeIfNeeded(em);
        }
    }

    @Override
    public List<T> next() {
        if (null == list) {
            hasNext();
        }
        List<T> lista = list;
        list = null;
        return lista;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * @return the lastResult
     */
    public int getLastResult() {
        return lastResult;
    }

    /**
     * @param lastResult the lastResult to set
     */
    public void setLastResult(int lastResult) {
        this.lastResult = lastResult;
    }
    
    class LessThanParams<T, Y extends Comparable<? super Y>> implements PredicateBuilder<T> {

        private final SingularAttribute<? super T, Y> field;
        private final Y value;
        private boolean lessOrEqualThan = false;

        public LessThanParams(SingularAttribute<? super T, Y> field, Y value, boolean lessOrEqualThan) {
            this.field = field;
            this.value = value;
            this.lessOrEqualThan = lessOrEqualThan;
        }

        @Override
        public Predicate build(CriteriaBuilder builder, Path<T> path) {
            if(lessOrEqualThan){
                return builder.lessThanOrEqualTo(path.get(field), value);
            }
            return builder.lessThan(path.get(field), value);
        }
    }
    
    class GreaterThanParams<T, Y extends Comparable<? super Y>> implements PredicateBuilder<T> {

        private final SingularAttribute<? super T, Y> field;
        private final Y value;
        private boolean greaterOrEqualThan = false;

        public GreaterThanParams(SingularAttribute<? super T, Y> field, Y value, boolean greaterOrEqualThan) {
            this.field = field;
            this.value = value;
            this.greaterOrEqualThan = greaterOrEqualThan;
        }

        @Override
        public Predicate build(CriteriaBuilder builder, Path<T> path) {
            if(greaterOrEqualThan){
                return builder.greaterThanOrEqualTo(path.get(field), value);
            }
            return builder.greaterThan(path.get(field), value);
        }
    }

    class BetweenParams<T, Y extends Comparable<? super Y>> implements PredicateBuilder<T> {

        private SingularAttribute<? super T, Y> field;
        private Y lower;
        private Y upper;

        public BetweenParams(SingularAttribute<? super T, Y> field, Y lower, Y upper) {
            this.field = field;
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public Predicate build(CriteriaBuilder builder, Path<T> path) {
            return builder.between(path.get(field), lower, upper);
        }
    }

    class EmptyParam<Y, C extends Collection<Y>> implements PredicateBuilder<T> {

        private String field;

        public EmptyParam(String field) {
            this.field = field;
        }

        @Override
        public Predicate build(CriteriaBuilder builder, Path<T> path) {
            return builder.isEmpty(path.<C>get(field));
        }
    }

    class NotEmptyParam<Y, C extends Collection<Y>> implements PredicateBuilder<T> {

        private String field;

        public NotEmptyParam(String field) {
            this.field = field;
        }

        @Override
        public Predicate build(CriteriaBuilder builder, Path<T> path) {
            return builder.isNotEmpty(path.<C>get(field));
        }
    }

    interface PredicateBuilder<T> {

        Predicate build(CriteriaBuilder builder, Path<T> path);
    }
}
